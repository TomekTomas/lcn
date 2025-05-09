
#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
lodz_alcohol_map_v6.py
======================

Wersja 6 — dynamiczna tabela osiedli:
  • Gdy zaznaczasz/odznaczasz kategorie (Rodzaj), liczby w tabeli aktualizują się w locie.
  • Zachowuje: HeatMap, LayerControl, klik‑zoom, DataTables.

Użycie:
  python lodz_alcohol_map_v6.py lista_koncesji.xlsx lodz_osiedla.geojson \
         --name-col OSIEDLE --out mapa_v6.html
"""

import argparse, time, re, json
from pathlib import Path
import pandas as pd, geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm
from unidecode import unidecode
from geopy.geocoders import Nominatim, ArcGIS
from geopy.extra.rate_limiter import RateLimiter

###############################################################################
def normalize_address(addr: str) -> str:
    a = re.sub(r"\bul\.?", "", addr.strip(), flags=re.I)
    a = re.sub(r"\blok\.?\s*\S+", "", a, flags=re.I)
    a = re.sub(r"\bpaw\.?\s*\S+", "", a, flags=re.I)
    a = re.sub(r"\s+", " ", a)
    return unidecode(a).strip()

def geocode_addresses(df, address_col, city_hint, cache_path, min_delay=1.0):
    cache_path = Path(cache_path)
    cache = {}
    if cache_path.exists():
        cdf = pd.read_csv(cache_path)
        cache = {r['address']: (r['lat'], r['lon']) for _, r in cdf.iterrows()}

    osm = Nominatim(user_agent="lodz-alko-map", timeout=10)
    arc = ArcGIS(timeout=10)
    geo_osm = RateLimiter(osm.geocode, min_delay_seconds=min_delay, max_retries=3, error_wait_seconds=2)
    geo_arc = RateLimiter(arc.geocode, min_delay_seconds=min_delay, max_retries=3, error_wait_seconds=2)

    pbar = tqdm(total=df[address_col].nunique(), desc="Geocoding")
    for addr in df[address_col].dropna().unique():
        if addr in cache and not pd.isna(cache[addr][0]):
            pbar.update(1); continue
        q_full = f"{normalize_address(addr)}, {city_hint}"
        loc = geo_osm(q_full) or geo_arc(q_full)
        if not loc:
            street_only = re.sub(r"\s+\d+.*$", "", addr).strip()
            q_street = f"{normalize_address(street_only)}, {city_hint}"
            loc = geo_osm(q_street) or geo_arc(q_street)
        cache[addr] = (loc.latitude, loc.longitude) if loc else (float('nan'), float('nan'))
        if len(cache)%100==0: _save_cache(cache,cache_path)
        pbar.update(1)
    pbar.close(); _save_cache(cache,cache_path)
    df['lat'] = df[address_col].map(lambda a: cache.get(a,(float('nan'),float('nan')))[0])
    df['lon'] = df[address_col].map(lambda a: cache.get(a,(float('nan'),float('nan')))[1])
    return df

def _save_cache(c,path): pd.DataFrame([{'address':k,'lat':v[0],'lon':v[1],'timestamp_iso':time.strftime('%Y-%m-%dT%H:%M:%S')} for k,v in c.items()]).to_csv(path,index=False)

###############################################################################
def to_gdf(df):
    return gpd.GeoDataFrame(df, geometry=[Point(xy) if not (pd.isna(xy[0]) or pd.isna(xy[1])) else None for xy in zip(df['lon'],df['lat'])], crs='EPSG:4326').dropna(subset=['geometry'])

def spatial_join(pts, polys, name_col):
    j = gpd.sjoin(pts, polys[[name_col,'geometry']], how='left', predicate='within')
    return j.rename(columns={name_col:'Osiedle'})

###############################################################################
# HTML helpers
###############################################################################
def build_sidebar(counts_df):
    tbody = "".join(f"<tr data-os='{r['Osiedle']}'><td>{r['Osiedle']}</td><td class='cnt' style='text-align:right'>{int(r['count'])}</td></tr>" for _,r in counts_df.iterrows())
    return f"""<div id='sidebar' style='position:fixed;top:70px;left:10px;width:280px;max-height:80vh;overflow:auto;z-index:999999;background:white;border-radius:8px;padding:10px 14px;box-shadow:0 0 15px rgba(0,0,0,.2);font-size:14px;'>
  <h4 style='margin:0 0 6px 0;'>Punkty z alkoholem<br><small>wg osiedli</small></h4>
  <table id='osTable' class='display' style='width:100%'>
    <thead><tr><th>Osiedle</th><th>Liczba</th></tr></thead>
    <tbody>{tbody}</tbody>
  </table>
  <hr>
  <h4 style='margin:6px 0;'>Filtr typu</h4>
  <div id='typeFilters'></div>
</div>
<link rel='stylesheet' href='https://cdn.datatables.net/1.13.7/css/jquery.dataTables.min.css'>
<script src='https://code.jquery.com/jquery-3.7.0.min.js'></script>
<script src='https://cdn.datatables.net/1.13.7/js/jquery.dataTables.min.js'></script>"""

def build_interactivity_js(categories, counts_json):
    cats = ",".join(json.dumps(c) for c in categories)
    counts_js = json.dumps(counts_json)
    return f"""<script>
$(function(){{
  var table = $('#osTable').DataTable({{ paging:false, order:[[1,'desc']] }});
  var osLayers={{}};
  map.eachLayer(function(l){{ if(l.feature && l.feature.properties && l.feature.properties.Osiedle) osLayers[l.feature.properties.Osiedle]=l;}});
  $('#osTable tbody').on('click','tr',function(){{
     var os=$(this).data('os'); var layer=osLayers[os];
     if(layer){{ map.fitBounds(layer.getBounds()); layer.setStyle({{weight:3,color:'#000'}}); setTimeout(()=>geojson.resetStyle(layer),1500); }}
  }});

  // Kategorie
  var cats=[{cats}];
  cats.forEach(c=>$('#typeFilters').append(`<div><label><input type="checkbox" class="cat-filter" value="${{c}}" checked> ${{c}}</label></div>`));

  var countsData={counts_js};
  var selectedCats=new Set(cats);

  function updateCounts(){{
     $('#osTable tbody tr').each(function(){{
        var os=$(this).data('os');
        var total=0;
        selectedCats.forEach(c=>{{ if(countsData[os] && countsData[os][c]) total+=countsData[os][c]; }});
        $('td.cnt',this).text(total);
     }});
     table.order([[1,'desc']]).draw(false);
  }}

  $('.cat-filter').on('change',function(){{
     var cat=$(this).val();
     if(this.checked) selectedCats.add(cat); else selectedCats.delete(cat);
     // toggle layer visibility
     if(catLayers[cat]) {{
        if(this.checked) map.addLayer(catLayers[cat]); else map.removeLayer(catLayers[cat]);
     }}
     updateCounts();
  }});
}});
</script>"""


###############################################################################
def build_map(pts, osd, name_col, start, outfile):
    import folium, json
    from folium.plugins import MarkerCluster, HeatMap

    m = folium.Map(location=start, zoom_start=12, tiles='CartoDB positron')

    # counts per osiedle total
    counts = pts.groupby('Osiedle').size().reset_index(name='count')
    os_join = osd.rename(columns={name_col:'Osiedle'}).merge(counts, on='Osiedle', how='left').fillna({'count':0})

    global geojson
    geojson = folium.GeoJson(os_join.to_json(), name='Granice',
                             style_function=lambda x:{'fillOpacity':0,'weight':1,'color':'black'},
                             tooltip=folium.features.GeoJsonTooltip(fields=['Osiedle','count'], aliases=['Osiedle','Liczba'])
                             ).add_to(m)

    folium.Choropleth(geo_data=os_join.to_json(),
                      data=os_join, columns=['Osiedle','count'],
                      key_on='feature.properties.Osiedle',
                      fill_color='YlOrRd', fill_opacity=0.6, line_opacity=0.4,
                      legend_name='Liczba punktów', name='Gęstość').add_to(m)

    # Heat map
    HeatMap([[r.geometry.y, r.geometry.x] for _,r in pts.iterrows()],
            name='Heat mapa', radius=15, blur=10).add_to(m)

    categories = sorted(pts['Rodzaj'].dropna().unique())
    cat_layers_js=[]
    cat_layer_ids={}
    counts_per_os_cat = pts.pivot_table(index='Osiedle', columns='Rodzaj', aggfunc='size', fill_value=0).to_dict(orient='index')

    for cat in categories:
        fg = MarkerCluster(name=cat)
        subset=pts[pts['Rodzaj']==cat]
        for _,r in subset.iterrows():
            folium.Marker([r.geometry.y, r.geometry.x],
                          popup=f"<b>{r.get('Nazwa punktu','')}</b><br>{r.get('Adres punktu','')}<br>{r.get('Rodzaj','')}<br><i>{r.get('Osiedle','')}</i>").add_to(fg)
        fg.add_to(m)
        cat_layers_js.append(f"catLayers['{cat}']=map._layers[{fg._id}];")

    folium.LayerControl().add_to(m)

    # Add sidebar + JS
    m.get_root().html.add_child(folium.Element(build_sidebar(counts.sort_values('count',ascending=False))))
    m.get_root().html.add_child(folium.Element("<script>var catLayers={{}};</script>" + ''.join(cat_layers_js)))
    m.get_root().html.add_child(folium.Element(build_interactivity_js(categories, counts_per_os_cat)))

    m.save(outfile)
    print(f"✔ Mapa zapisana → {outfile}")

###############################################################################
def parse_args():
    p=argparse.ArgumentParser(description="Mapa punktów z alkoholem – Łódź (v6 dynamic counts).")
    p.add_argument('excel'); p.add_argument('osiedla')
    p.add_argument('--out', default='mapa_v6.html'); p.add_argument('--cache', default='geocode_cache.csv')
    p.add_argument('--name-col', default='name'); return p.parse_args()

def main():
    a=parse_args()
    print("👉 Wczytywanie Excela…")
    df=pd.read_excel(a.excel)
    if 'lat' not in df.columns or 'lon' not in df.columns:
        print("👉 Geokodowanie…")
        df=geocode_addresses(df,'Adres punktu','Łódź, Polska',a.cache)
    pts=to_gdf(df)
    osd=gpd.read_file(a.osiedla)
    pts=spatial_join(pts,osd,a.name_col)
    cent=pts.geometry.unary_union.centroid if not pts.empty else Point(19.458,51.759)
    build_map(pts,osd,a.name_col,(cent.y,cent.x),a.out)

if __name__=='__main__':
    main()
