"""Execute actual extracted helper ASTs with a fake store; not database coverage."""
import argparse, ast, asyncio, subprocess, sys, types
from pathlib import Path
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument("--repo",type=Path,required=True)
parser.add_argument("--baseline",default="8e8e9dc9cde79c4053befae35d84119d6b3b7cde")
args=parser.parse_args();repo=args.repo.resolve()
p="hindsight-api-slim/hindsight_api/engine/retain/fact_storage.py"
base=subprocess.check_output(["git","-C",str(repo),"show",args.baseline+":"+p],text=True,encoding="utf-8")
new=(repo/p).read_text(encoding="utf-8")
class Store:
 async def relabel_document_memories(self, **kw):
  self.final=[kw["final_tags"](x) for x in [["topic:first","status:reviewed"],["topic:first","status:draft"]]]
  self.meta=kw["metadata"];self.scopes=kw["observation_scopes"]
  return types.SimpleNamespace(rescoped_unit_ids=[],updated=2)
store=Store()
mem=types.ModuleType("hindsight_api.engine.memories");mem.get_memories=lambda:store
labels=types.ModuleType("hindsight_api.engine.retain.entity_labels");labels.split_label_tags=lambda tags,keys:[]
sys.modules[mem.__name__]=mem;sys.modules[labels.__name__]=labels
def load(src):
 node=next(n for n in ast.parse(src).body if isinstance(n,ast.AsyncFunctionDef) and n.name=="update_memory_units_metadata_and_tags")
 mod=ast.Module(body=[ast.ImportFrom(module="__future__",names=[ast.alias(name="annotations")],level=0),node],type_ignores=[])
 ns={"__package__":"hindsight_api.engine.retain","fq_table":lambda name:name}
 exec(compile(ast.fix_missing_locations(mod),"<actual-source-helper>","exec"),ns)
 return ns[node.name]
async def run():
 expected=[["topic:first","status:reviewed"],["topic:first","status:draft"]]
 for label,fn in [("upstream",load(base)),("candidate",load(new))]:
  for mode,tags in [("different",["topic:second"]),("omitted",[]),("empty",[])]:
   kw={"observation_scopes":"shared"}
   if label=="candidate":kw["append"]=True
   await fn(None,"synthetic-bank","doc",tags,{"source":"test"},**kw)
   kept=store.final==expected
   assert kept==(label=="candidate"),(label,mode,store.final)
   assert store.meta=={"source":"test"} and store.scopes=="shared"
   print(label,mode,"preserved" if kept else "overwritten")
 fn=load(new)
 await fn(None,"synthetic-bank","doc",["topic:replacement"],{},append=False)
 assert store.final==[["topic:replacement"],["topic:replacement"]]
 print("candidate non-append keeps native replacement behavior")
asyncio.run(run())
