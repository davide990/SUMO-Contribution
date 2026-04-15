#!/usr/bin/env python3
#
# -----------------------------------------------------------------------------
#  License: MIT
#  Author: Davide Guastella (LIS)
#  Date: 2026-04-15
#
#  Description:
#  This script parses a SUMO routes XML file, checks each vehicle’s route
#  against a given SUMO network, and extracts the longest consecutive
#  subsequence of valid edges. Vehicles whose routes contain no valid edges
#  are removed. The script outputs:
#     1. A new routes XML file with cleaned routes
#     2. A text file listing the IDs of removed vehicles
# -----------------------------------------------------------------------------

import xml.etree.ElementTree as ET
import sumolib


def longest_valid_subsequence(edges, net):
    """
    Return the longest consecutive subsequence of edges
    that exist in the SUMO network.
    """
    longest = []
    current = []

    for e in edges:
        try:
            net.getEdge(e)  # valid edge
            current.append(e)
        except Exception:
            if len(current) > len(longest):
                longest = current
            current = []

    if len(current) > len(longest):
        longest = current

    return longest


def process_routes(routes_file, net_file, output_routes, removed_ids_file):
    """
    Process the input routes file:
      - Replace each vehicle's route with its longest valid subsequence
      - Remove vehicles with no valid edges
      - Write cleaned routes and removed vehicle IDs to output files
    """
    net = sumolib.net.readNet(net_file)

    tree = ET.parse(routes_file)
    root = tree.getroot()

    removed_ids = []

    for veh in root.findall("vehicle"):
        route_elem = veh.find("route")
        if route_elem is None:
            removed_ids.append(veh.get("id"))
            root.remove(veh)
            continue

        edges = route_elem.get("edges").split()
        valid_seq = longest_valid_subsequence(edges, net)

        if len(valid_seq) == 0:
            removed_ids.append(veh.get("id"))
            root.remove(veh)
        else:
            route_elem.set("edges", " ".join(valid_seq))

    tree.write(output_routes, encoding="UTF-8", xml_declaration=True)

    with open(removed_ids_file, "w") as f:
        for vid in removed_ids:
            f.write(vid + "\n")

    return removed_ids


if __name__ == '__main__':
    # -----------------------------
    # Example usage
    # -----------------------------
    # the INPUT route file
    routes_file = "/Users/dguastel/Desktop/scenarios/PALERMO/SHAPE_FILE/sumo/rou.veh.xml"
    # the TARGET route file
    net_file = "/Users/dguastel/Desktop/scenarios/PALERMO/SHAPE_FILE/sumo/subpa.xml"
    # the OUTPUT route file
    output_routes = "filtered_routes.xml"
    # list of IDs of vehicles removed in the target route file
    removed_ids_file = "removed_vehicles.txt"

    removed = process_routes(routes_file, net_file, output_routes, removed_ids_file)
    print(f"Removed {len(removed)} vehicles.")
