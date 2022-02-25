import pandas as pd
import requests
import os
import json
import hashlib


PHENX_PROTOCOL_FILE_URL = "https://www.phenxtoolkit.org/toolkit_content/documents/resources/Protocol_cross_reference.xlsx"
DATA_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'data')


def calculate_hash(file):
    sha256_hash = hashlib.sha256()
    output_name = os.path.basename(file) + '.sha256'

    with open(file, "rb") as f:
        for block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(block)
    with open("current.sha256", 'w') as filesha:
        filesha.write(sha256_hash.hexdigest())
    return sha256_hash.hexdigest()


def download_file(file_url):
    os.makedirs(DATA_DIR, exist_ok=True)
    output_file_name = os.path.join(DATA_DIR, file_url.split('/')[-1])
    with requests.get(file_url, stream=True) as r:
        r.raise_for_status()
        with open(output_file_name, 'wb') as output_stream:
            for chunk in r.iter_content(chunk_size=8192):
                output_stream.write(chunk)
    return output_file_name


def get_xslx_row(file_name):
    """
    Xslx row generator
    :param file_name: xslx file path
    :return: a single row on each call
    """
    file_stream = pd.read_excel(file_name)
    df = pd.DataFrame(file_stream)
    cols = df.columns
    for row in df.iterrows():
        row_dict = {}
        for col_name in cols:
            if pd.notna(row[1][col_name]):
                row_dict[col_name] = str(row[1][col_name]).strip()
        yield row_dict


def create_nodes_and_edges(file_name):
    nodes = {}
    edges = {}
    dup_edges = 0
    row_count = 0
    for row in get_xslx_row(file_name):
        protocol_node = {
            'id': row['Protocol ID'],
            'name': row['Protocol Name'],
            'type': 'protocol'
        }
        other_node = {
            "id": row['Standard ID'],
            "type": row['Standard Type'],
            "name": row['Standard Description']
        }
        edge_type = "biolink:association"
        edge_id_p_o = hashlib.sha256(f"{protocol_node['id']}-{edge_type}-{other_node['id']}".encode('utf-8')).hexdigest()
        edge_id_o_p = hashlib.sha256(f"{other_node['id']}-{edge_type}-{protocol_node['id']}".encode('utf-8')).hexdigest()
        if edge_id_o_p in edges:
            dup_edges += 1
        if edge_id_p_o in edges:
            dup_edges += 1
        if other_node['id'] == "62293-6":
            print('pause')
        edges[edge_id_p_o] = {
            "id":  edge_id_p_o,
            "subject": protocol_node['id'],
            "object": other_node['id'],
            "predicate": "biolink:association",
            "provided_by": "helx_phenx_kgx_convertor"
        }
        edges[edge_id_o_p] = {
            "id": edge_id_o_p,
            "object": protocol_node['id'],
            "subject": other_node['id'],
            "predicate": "biolink:association",
            "provided_by": "helx_phenx_kgx_convertor"
        }
        nodes[protocol_node['id']] = protocol_node
        nodes[other_node['id']] = other_node
        row_count += 1
    print("processed rows:", row_count)
    print("duplicate edges found:", dup_edges)
    return nodes, edges


def normalize_nodes(nodes):
    PROTOCOL_TYPE = 'protocol'
    CDE_TYPE = 'caDSR Common Data Elements (CDE)'
    PHENOTYPE_TYPE = 'Human Phenotype Ontology'
    LOINC_TYPE = "Logical Observation Identifiers Names and Codes (LOINC)"
    protocols = {}
    cdes = {}
    phenotypes = {}
    loincs = {}
    for node_id, node in nodes.items():
        if node['type'] == PHENOTYPE_TYPE:
            phenotypes[node_id] = node
        if node['type'] == CDE_TYPE:
            cdes[node_id] = node
        if node['type'] == PROTOCOL_TYPE:
            protocols[node_id] = node
        if node['type'] == LOINC_TYPE:
            loincs[node_id] = node

    print(f'len protocols {len(protocols)}')
    print(f'len cdes {len(cdes)}')
    print(f'len phenotypes {len(phenotypes)}')
    phenotypes, non_normalized = normalize_phenotypes(phenotype_nodes=phenotypes)
    cde_nodes = normalize_cde_nodes(cdes)
    protocols = normalize_phenx_nodes(protocols)
    loincs = normalize_loinc_nodes(loincs)
    final = {}
    final.update(protocols)
    final.update(cde_nodes)
    final.update(phenotypes)
    final.update(loincs)

    return final



def normalize_loinc_nodes(loinc_nodes):
    category = ['biolink:Publication']
    for id_, node in loinc_nodes.items():
        id_new = f"LOINC:{id_}"
        name = node['name']
        url = f"https://loinc.org/{id_}/"
        print(url)
        description = node["name"]
        loinc_nodes[id_] = {
            "id": id_new,
            "category": category,
            "name": name,
            "resource_url": url,
            "description": description
        }
    return loinc_nodes


def normalize_cde_nodes(cde_nodes):
    category = ['biolink:Publication']
    for id_, node in cde_nodes.items():
        id_new = f"caDSRCDE:{id_}"
        name = node['name']
        url = f"https://cdebrowser.nci.nih.gov/cdebrowserClient/cdeBrowser.html#/search?publicId={id_}&version=1.0"
        description = node["name"]
        cde_nodes[id_] = {
            "id": id_new,
            "category": category,
            "name": name,
            "resource_url": url,
            "description": description
        }
    return cde_nodes

def normalize_phenx_nodes(phenx_nodes):
    category = ['biolink:Publication']
    for id_ , node in phenx_nodes.items():
        id_new = f"phenx:{id_}"
        url = f"https://www.phenxtoolkit.org/search/results?searchTerm={id_}&searchtype=smartsearch"
        print(url)
        name = node["name"]
        description = node["name"]
        phenx_nodes[id_] = {
            "id": id_new,
            "category": category,
            "name": name,
            "resource_url": url,
            "description": description
        }
    return phenx_nodes


def normalize_edges(normalized_nodes, edges):
    for e_id, edge in edges.items():
        edge['subject'] = normalized_nodes[edge['subject']]['id']
        edge['object'] = normalized_nodes[edge['object']]['id']
    return edges


def normalize_phenotypes(phenotype_nodes):
    nn_url = "https://nodenormalization-sri.renci.org/get_normalized_nodes"
    # process in 100 batches
    # do some correction for curies
    non_normalized = []
    for i in range(0, len(phenotype_nodes), 100):
        curies = list(phenotype_nodes.keys())[i:i+100]
        response = requests.post(nn_url, json={"curies": curies})
        if response.status_code == 502:
            response = requests.post(nn_url, json={"curies": curies})
        normalized = response.json()
        for curie in curies:
            if curie not in normalized or not normalized[curie]:
                print(curie, 'did not normalize setting it as a biolink:PhenotypicFeature')
                non_normalized.append(curie)
                phenotype_nodes[curie] = {
                    "id": curie,
                    "name": phenotype_nodes[curie]['name'],
                    "category": "biolink:PhenotypicFeature",
                    "equivalent_identifiers": [curie]
                }
            else:
                nn_node = normalized[curie]
                categories = nn_node['type']
                synonyms = list(filter(lambda y: y, [x.get('label') for x in nn_node['equivalent_identifiers']]))
                equivalent_ids = [x['identifier'] for x in nn_node['equivalent_identifiers']]
                name = nn_node['id'].get('label')
                id = nn_node['id'].get('identifier')
                phenotype_nodes[curie] = {
                    "id": id,
                    "name": name,
                    "category": categories,
                    "synonyms": synonyms,
                    "equivalent_identifiers": equivalent_ids
                }
    return phenotype_nodes , non_normalized

def write_json_l(dictionary, file_name):
    with open(file_name, 'w') as stream:
        for x , item in dictionary.items():
            stream.write(json.dumps(item) + '\n')



if __name__ == '__main__':
    file_path = download_file(PHENX_PROTOCOL_FILE_URL)
    sha = print(calculate_hash(file_path))
    nodes, edges = create_nodes_and_edges(file_path)
    normalized_nodes = normalize_nodes(nodes)
    normalized_edges = normalize_edges(normalized_nodes, edges)
    write_json_l(normalized_edges, os.path.join(DATA_DIR, 'edges.jsonl'))
    write_json_l(normalized_nodes, os.path.join(DATA_DIR, 'nodes.jsonl'))