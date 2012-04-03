import sys
import pyes

q = {"builder": sys.argv[1],
     "buildnumber": sys.argv[2],
     "step": sys.argv[3]}

conn = pyes.ES("localhost:9200")
tq = pyes.BoolQuery()
for k, v in q.iteritems():
    tq.add_must(pyes.TermQuery(k, v))
ctq = pyes.TermsQuery()
ctq.add("channel", ["stdout","header","json"])
tq.add_must(ctq)
data = conn.search(pyes.Search(query=tq,sort=['block'])) #, fields=["block","content"]

steps = set()
for hit in data['hits']['hits']:
    #print hit['_source']['block'], hit['_source']['name'], hit['_source']['step'], hit['_source']['buildnumber']
    #steps.add(hit['_source']['step'])
    sys.stdout.write(hit['_source']['content'])

