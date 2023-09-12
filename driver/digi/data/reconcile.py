import os
import time
from digi.data import logger
import digi

def extract_key(policy: str):
    parts = policy.split(" => ")
    if parts[0].startswith("has"):
        return parts[0][4:-1]
    
    raise Exception(f'unknown type reconcile policy key {parts[0]}')

def generate_reconcile_flow(in_flow, source, dest, pool2gvr, policies):
    source_pool, source_branch = source.split("@")
    g, v, r = pool2gvr[source_pool].split("/")
    tp1, tp2, tp3 = digi.util.get_spec(g, v, r, source_pool, "default")
    logger.info(f"sync: make query {source}--{dest}, spec of {source} is {tp1}")
    schemas = tp1['egress'][source_branch]['schemas']
    logger.info(f"sync: make query {source}--{dest} schemas of {source} are {schemas}")
    logger.info(f"sync: make query {source}--{dest} policies are {policies}")
    recon_flow = f"switch ("
    for policy in policies:
        policy_key = extract_key(policy)
        for schema in schemas:
            if policy_key in schema:
                recon_flow += f"case {policy} "
                break
    if recon_flow != f"switch (":
        in_flow = recon_flow + ") | " + in_flow

    return in_flow

def update_schemas(records, source, dest):
    dest_pool, dest_branch = dest.split('@')
    tp1, tp2, tp3 = digi.util.get_spec(digi.g, digi.v, digi.r, dest_pool, "default")
    schemas = tp1['egress'][dest_branch]['schemas']
    logger.info(f"sync: load {source}--{dest}, schemas are {schemas}")

    for record in records:
        if list(record.keys()) not in schemas:
            logger.info(f"sync: load {source}--{dest} schema of {dest} add {list(record.keys())}")
            schemas.append(list(record.keys()))
    tp1['egress'][dest_branch]['schemas'] = schemas
    resp, e = digi.util.patch_spec(digi.g, digi.v, digi.r, dest_pool, "default", tp1, rv=tp2)
    if e is not None:
        logger.warning(f"sync: load {source}--{dest} fail to update schemas {schemas}")