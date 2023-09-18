import os
import time
from digi.data import logger
import digi
from langchain.chat_models import ChatOpenAI
from langchain.prompts.chat import (
    ChatPromptTemplate,
    SystemMessagePromptTemplate,
    HumanMessagePromptTemplate,
)
from langchain.chains import LLMChain
from langchain.schema import BaseOutputParser

class CommaSeparatedListOutputParser(BaseOutputParser):
    """Parse the output of an LLM call to a comma-separated list."""

    def parse(self, ans):
        ret = []
        ans = ans.strip()[1:-1].split(",")
        for an in ans:
            ret.append(an.strip()[1:-1])

        return ret

template = """You are a helpful assistant who finds the relation between the fields in the egress schemas and the fields in the one ingress (expected) schema, and try to map the egress schemas to the ingress schema, and just map a egress schema to the ingress schema (do not map a egress schema to another egress schema). You will get a list of egress schemas {egress_schemas} and one ingress schema {ingress_schema}. Every schema will be represented in a list of fields. Your mapping result should be a list of mapping rules in strings. Every mapping rule is in a strict format, in which you have two tools: rename and put. You can consider the input follows the format of "number unit" (e.g. 1.5 cm). "rename" change the name of the unit. For example, if we use "rename km:=cm" on "1.5 cm", we get "1.5 km". "put" change the value of the number for a speciic unit. For example, if we use "put cm:=cm*1000" on "1.5 cm", we get "1500 cm". Please precisely remember the usage of "rename" and "put".
There are three kinds of mapping. First, different kind of units for the same thing. For example, if a egress schema contains a field 'Celsius' and the ingress schema contains a field 'Fahrenheit', you should add 'has(Celsius) => put Celsius:=Celsius*9/5+32 | rename Fahrenheit:=Celsius'. This is because both Celsius and Fahrenheit are used to express temperature. You should find the right formula to converse Celsius to Fahrenheit and change the name of the unit. Another example is that if a egress schema contains a field 'mW' and the ingress schema contains a field 'watt', you should add 'has(mW) => put mW:=mW/1000 | rename watt:=mW'. 
The second kind is different names for the same unit. If a egress schema contains a field 'kilometre' and the ingress schema contains a field 'km', you should add 'has(kilometre) => rename km:=kilometre'. This is because km and kilometre are the same thing. 
The third kind is exactly the same name. For a field in a egress schema that is named the same as in the ingress schema, ignore it because this field is already matched. 
The output list should be strictly represented in a python list (e.g. ['rule 1', 'rule 2', 'rule 3']), and nothing else. Please actively find the matching fields other than the examples I gave you. The mapping is strictly from a egress schema to the ingress schema, not the other way around. The fields in the schemas are highly likely to be the units in physics. Please focus on them. I want the mapping rules between all possible matching fields. Please do not omit any of them. Try all pairs if necessary."""
system_message_prompt = SystemMessagePromptTemplate.from_template(template)

chat_prompt = ChatPromptTemplate.from_messages([system_message_prompt])

chain = LLMChain(
    llm=ChatOpenAI(),
    prompt=chat_prompt,
    output_parser=CommaSeparatedListOutputParser()
)

def extract_key(policy: str):
    parts = policy.split(" => ")
    if parts[0].startswith("has"):
        return parts[0][4:-1]
    
    raise Exception(f'unknown type reconcile policy key {parts[0]}')

# def generate_reconcile_flow(in_flow, source, dest, pool2gvr, policies):
#     source_pool, source_branch = source.split("@")
#     g, v, r = pool2gvr[source_pool].split("/")
#     spec, rv, gen = digi.util.get_spec(g, v, r, source_pool, "default")
#     logger.info(f"sync: make query {source}--{dest}, spec of {source} is {spec}")
#     # schemas = spec['egress'][source_branch]['schemas']
#     schemas = spec['egress'][source_branch].get('schemas',[])
#     logger.info(f"sync: make query {source}--{dest} schemas of {source} are {schemas}")
#     logger.info(f"sync: make query {source}--{dest} policies are {policies}")
#     recon_flow = f"switch ("
#     for policy in policies:
#         policy_key = extract_key(policy)
#         for schema in schemas:
#             if policy_key in schema:
#                 recon_flow += f"case {policy} "
#                 break

#     if recon_flow != f"switch (":
#         in_flow = recon_flow + "default => drop) | " + in_flow

#     return in_flow

def generate_reconcile_flow(in_flow, source, dest, pool2gvr, policies, ingress_schemas):
    source_pool, source_branch = source.split("@")
    g, v, r = pool2gvr[source_pool].split("/")
    spec, rv, gen = digi.util.get_spec(g, v, r, source_pool, "default")
    logger.info(f"sync: make query {source}--{dest}, spec of {source} is {spec}")
    # schemas = spec['egress'][source_branch]['schemas']
    schemas = spec['egress'][source_branch].get('schemas',[])
    logger.info(f"sync: make query {source}--{dest} egress_schemas of {source} are {schemas}")
    logger.info(f"sync: make query {source}--{dest} ingress_schema is {ingress_schemas}")

    ans = chain.run(egress_schemas=schemas, ingress_schema=ingress_schemas[0])
    logger.info(f"reconcile: generate_reconcile_flow {source}--{dest} dataflow is {ans}")

    recon_flow = f"switch ("
    for an in ans:
        recon_flow += f"case {an} "

    in_flow = recon_flow + "default => pass) | " + in_flow

    return in_flow

def update_schemas(records, source, dest):
    dest_pool, dest_branch = dest.split('@')
    spec, rv, gen = digi.util.get_spec(digi.g, digi.v, digi.r, dest_pool, "default")
    # schemas = spec['egress'][dest_branch]['schemas']
    schemas = spec['egress'][dest_branch].get('schemas',[])
    logger.info(f"sync: load {source}--{dest}, schemas are {schemas}")

    for record in records:
        if list(record.keys()) not in schemas:
            logger.info(f"sync: load {source}--{dest} schema of {dest} add {list(record.keys())}")
            schemas.append(list(record.keys()))
    spec['egress'][dest_branch]['schemas'] = schemas
    resp, e = digi.util.patch_spec(digi.g, digi.v, digi.r, dest_pool, "default", spec, rv=rv)
    if e is not None:
        logger.warning(f"sync: load {source}--{dest} fail to update schemas {schemas}")