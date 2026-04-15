import geopandas as gpd
import sumolib
import os
import subprocess
import argparse
import tempfile


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Convert GeoJSON zones to SUMO TAZ XML format."
    )
    parser.add_argument("--sumo-network", type=str, required=True, help="Path to the SUMO network file (.net.xml)")
    parser.add_argument("--geojson-input", type=str, required=True, help="Path to the input GeoJSON file")
    parser.add_argument("--geojson-id-column", type=str, required=False,
                        help="Column name in the GeoJSON to use as TAZ ID")
    parser.add_argument("--output-taz", type=str, required=True, help="Path to the output TAZ XML file")
    return parser.parse_args()


if __name__ == '__main__':
    polyconvert = sumolib.checkBinary("polyconvert")
    tmp_poly = tempfile.NamedTemporaryFile(suffix='.xml', delete=True)
    tmp_shp = tempfile.NamedTemporaryFile(suffix='.shp', delete=True)
    args = parse_arguments()
    gdf = gpd.read_file(args.geojson_input)
    gdf.to_file(tmp_shp.name)

    with open(tmp_poly.name, 'w') as f:
        with open(tmp_poly.name, 'w') as f:
            polyconvert_opts = [
                polyconvert,
                "--shapefile-prefix", tmp_shp.name.split('.')[0],
                "--shapefile.guess-projection", "true",
                "--shapefile.traditional-axis-mapping", "true",
                "-n", args.sumo_network,
                "--shapefile.add-param", "true",
                "-o", tmp_poly.name
            ]
            subprocess.call(polyconvert_opts)

            edgesInDistrictsOpts = [
                "-n", args.sumo_network,
                '-t', tmp_poly.name,
                '-o', args.output_taz,
                "-s",
                "--complete"
            ]
            subprocess.call(["python", os.path.join(os.environ['SUMO_HOME'], 'tools', 'edgesInDistricts.py'),
                             *edgesInDistrictsOpts])
