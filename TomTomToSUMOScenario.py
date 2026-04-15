import os
import sys
from dataclasses import dataclass
import numpy as np
import pandas as pd
import sumolib
from datetime import datetime

if 'SUMO_HOME' in os.environ:
    sys.path.append(os.path.join(os.environ['SUMO_HOME'], 'tools', 'detector'))
    import mapDetectors
else:
    sys.exit("please declare environment variable 'SUMO_HOME'")


@dataclass
class EdgeDataAdditional:
    """
    This corresponds to an "EdgeData" definition in the edge data XML additional file
    """
    ID: str
    fname: str
    begin: int = 0
    end: int = 0
    excludeEmpty: bool = True


def write_edgedata_add_to_file(output_path, ts_sumo: list[int]) -> None:
    """
    Write the edgedata additional XML file for the given the list of aggregation instants (in ts_sumo).

    :param output_path: where the output XML FILE is written
    :param ts_sumo: the list of aggregation instants
    :return:
    """
    ll = []
    for i in range(len(ts_sumo) - 1):
        ll.append(
            EdgeDataAdditional(f"{ts_sumo[i]}_to_{ts_sumo[i + 1]}",
                               f'edgedata_{ts_sumo[i]}_to_{ts_sumo[i + 1]}.out.xml',
                               ts_sumo[i],
                               ts_sumo[i + 1], True))
    # generate_edgedata_add(os.path.join(output_path, 'edgedata.add.xml', ), ll)
    edgedata = """    <edgeData id="{the_id}" file="{fname}" begin="{begin}" end="{end}" excludeEmpty="{excludeEmpty}"/>\n"""

    with open(output_path, "w", encoding="utf8") as outfile:
        sumolib.xml.writeHeader(outfile, root="additional")
        for ed in ll:
            outfile.write(
                edgedata.format(
                    the_id=ed.ID,
                    fname=ed.fname,
                    begin=str(ed.begin),
                    end=str(ed.end),
                    excludeEmpty=str(ed.excludeEmpty)
                )
            )
        outfile.write("</additional>\n")


def traffic_data_to_sumo_edgedata_count(sumo_net, df, timestamps_list, ts_sumo_list, fname="interpolated.edgedata.xml",
                                        is_edge=True):
    """
    Generate edgedata file that can be provided to RouteSampler tool in SUMO to calibrate traffic

    This method takes in input the DF, where the index column contains the time stamps, the columns the
    SUMO edges ID where the traffic count is observed, and the values of the table is the traffic count
    (that is, the amount of vehicles). The output is written to an output xml file
    :param df: the DATAFRAME
    :param timestamps_list: list of Timestamps (index of df)
    :param ts_sumo_list: list of timestamps in seconds
    :param fname: output file
    :return:
    """
    fd = open(fname, "w")
    sumolib.xml.writeHeader(fd, "$Id$", "meandata")  # noqa

    df_time = df.copy(deep=True)
    df_time.sort_index(inplace=True)
    ts_list = list(df_time.index)  # to_list()

    # 00:00 corresponds to zero in simulation time. The start time in the sumo time instant should be
    # the distance between midnight and the first ts in the previous list. Contrarily, the simulation will be at 0
    # even if the input dataset doesn't start at midnight
    ts_midnight = datetime.combine(ts_list[0], datetime.min.time())
    start_time_sec = np.abs((ts_midnight - ts_list[0]).total_seconds())
    ts_sumo = [start_time_sec + (ts_other - ts_list[0]).total_seconds() for ts_other in ts_list]

    for idx, ts in enumerate(ts_list[:-1]):  # enumerate(timestamps_list[:-1]):
        ts_sumo_idx = ts_sumo[idx]
        ts_sumo_idx_next = ts_sumo[idx + 1]
        sub_df = df.iloc[idx, :]
        fd.write(f'\n   <interval begin="{ts_sumo_idx}" end="{ts_sumo_idx_next}">\n')

        for idx_j in sub_df.items():
            if not is_edge:
                edge_id = sumo_net.getLane(idx_j[0]).getEdge().getID()
            else:
                edge_id = idx_j[0]
            traf_count = idx_j[1]
            if traf_count is None or not np.isfinite(traf_count):
                traf_count = 0
            fd.write('      <edge id="%s" entered="%d"/>\n' % (edge_id, traf_count))
        fd.write(f' </interval>\n')
    fd.write("</meandata>\n")
    fd.close()


if __name__ == '__main__':
    """
    PLEASE NOTE: the road network MUST be converted from OSM using the --output.original-names option in netconvert
    
    I used this parameters:
    
    >osmconvert belgium-latest.osm.pbf -B=bruxelles_capitale.poly -o=bxl_capitale.osm  
    >netconvert --osm bxl_capitale.osm -o bxl_map.full_wattrib.xml --ramps.guess --tls.guess-signals --tls.discard-simple --tls.join --tls.default-type actuated -t $SUMO_HOME/data/typemap/osmNetconvert.typ.xml  --output.original-names --osm.sidewalks --osm.elevation --osm.all-attributes
    >netconvert -s bxl_map.net.xml -o bxl_roads_TEST.net.xml --ramps.guess --tls.guess-signals --tls.discard-simple --tls.join --tls.default-type actuated -t $SUMO_HOME/data/typemap/osmNetconvert.typ.xml  --output.street-names --output.original-names --remove-edges.by-type railway.tram,highway.service,railway.rail,railway.subway,highway.residential,highway.footway
    """
    sumo_net = "bxl_roads.net.xml"  # the file of the road network in SUMO format (net.xml)
    tomtom_parquet = "tomtom_february_davide.parquet"

    day = "2024-02-02"
    sensors_aggr_frequency = 3600
    out_folder = 'output_{}'.format(day)
    begin_edgedata = 0
    end_edgedata = 86400
    # --------------------------------------------------------------------
    os.makedirs(out_folder, exist_ok=True)
    df = pd.read_parquet(tomtom_parquet)
    df = df[['timestamp', 'osm_id', 'sampleSize']]
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    DT = df.pivot_table(index='timestamp', columns='osm_id', values='sampleSize')

    # --- Step 2: Parse osm.net.xml and build origId → edge ID map ---
    net = sumolib.net.readNet(sumo_net)
    nodes_edges_mapping = {(e.getFromNode().getID(), e.getToNode().getID()): e for e in net.getEdges()}
    osm_to_sumo = {}

    mapping_to_save = {}
    det_coordinates = []
    for osm_id in DT.columns:
        parts = osm_id.split("_")
        if len(parts) < 2:
            continue  # Skip invalid
        u, v = parts[0], parts[1]
        r = nodes_edges_mapping.get((u, v), None)
        if r:
            osm_to_sumo[osm_id] = r.getID()
            x, y = sumolib.geomhelper.positionAtShapeOffset(r.getShape(), r.getLength() / 2)
            lon, lat = net.convertXY2LonLat(x, y, False)
            det_coordinates.append((r.getID(), lon, lat))
            mapping_to_save[osm_id] = r.getLanes()[0].getID()

    unmapped_columns = set(DT.columns).difference(set(osm_to_sumo.keys()))
    DT_mapped = DT.drop(unmapped_columns, axis=1)
    DT_day = DT_mapped.loc[day]

    ddd = pd.DataFrame.from_dict(mapping_to_save, orient="index", columns=['lane_id'])
    ddd = ddd.rename_axis('sensor_id')
    ddd.to_csv(
        os.path.join(out_folder, "sensors_edges_mapping.csv"),
        sep=";")

    pd.DataFrame(det_coordinates, columns=['id', 'lon', 'lat']).to_csv(os.path.join(out_folder, "det_coordinates.csv"),
                                                                       sep=";")

    DT_day = DT_day.rename_axis('ts')
    DT_day = DT_day.tz_localize(None)
    DT_day.to_csv(os.path.join(out_folder, "count_dataset.csv"), sep=";")

    # now invoke sumo tools to make a full scenario
    ts_sumo = list(range(begin_edgedata, end_edgedata, sensors_aggr_frequency))
    write_edgedata_add_to_file(os.path.join(out_folder, "edgedata.add.xml"), ts_sumo)

    traffic_data_to_sumo_edgedata_count(sumo_net, DT_day, list(DT_day.index), ts_sumo,
                                        fname=os.path.join(out_folder, "interpolated.edgedata.xml"),
                                        is_edge=True)

    opts = ['-n', sumo_net,
            '-d', os.path.join(out_folder, "det_coordinates.csv"),
            '--interval', str(sensors_aggr_frequency),
            '-o', os.path.join(out_folder, "det.add.xml"),
            '--max-radius', str(3),
            '--delimiter', ';',
            '--all-lanes']
    mapDetectors.main(opts)
