from zope.interface import implements
import twisted.internet.interfaces
from twisted.protocols import basic
from glob import glob
import pyes
import os
import sys
from bz2 import BZ2File

mapping = {
    "content" : {
        "type" : "string"
    },
    "name" : {
      "index" : "not_analyzed",
      "type" : "string"
    },
    "builder" : {
      "index" : "not_analyzed",
      "type" : "string"
    },
    "block" : {
      "type" : "long"
    },
    "logname" : {
      "index" : "not_analyzed",
      "type" : "string"
    },
    "log_finished" : {
      "type" : "boolean"
    },
    "channel" : {
      "index" : "not_analyzed",
      "type" : "string"
    },
    "buildnumber" : {
      "type" : "long"
    },
    "step" : {
      "index" : "not_analyzed",
      "type" : "string"
    },
    "master" : {
      "index" : "not_analyzed",
      "type" : "string"
    },
    "channel_id" : {
      "type" : "long"
    }
}


class NullAddress(object):
    "an address for NullTransport"
    implements(twisted.internet.interfaces.IAddress)

class NullTransport(object):
    "a do-nothing transport to make NetstringReceiver happy"
    implements(twisted.internet.interfaces.ITransport)
    def write(self, data): raise NotImplementedError
    def writeSequence(self, data): raise NotImplementedError
    def loseConnection(self): pass
    def getPeer(self):
        return NullAddress
    def getHost(self):
        return NullAddress

class LogScanner(basic.NetstringReceiver):
    channels = ["stdout", "stderr", "header", None, None, "json"]
    def __init__(self, chunk_cb):
        self.chunk_cb = chunk_cb
        #self.makeConnection(NullTransport())
    def stringReceived(self, line):
        channel = int(line[0])
        self.chunk_cb(self.channels[channel], channel, line[1:])


def main():
    files=glob("compare/*")
    chunks = []; builder=None; step=None; buildnumber=None; logname=None; name=None
    def chunker(channel, channel_id, content):
        chunks.append({"channel":channel,"channel_id":channel_id,"block":len(chunks),
                       "content":content,
                       "master":"l10n-master",
                       "builder":builder, "buildnumber": buildnumber,
                       "step": step,
                       "logname": logname, "name": name})
    ls = LogScanner(chunker)
    conn = pyes.ES("localhost:9200")
    try:
        conn.create_index_if_missing('l10n-master')
        conn.put_mapping('logs',{'properties':mapping},['l10n-master'])
    except Exception, e:
        #print e
        pass
    for _dir in sys.argv[1:]:
        print _dir
        for dirpath, dirnames, files in os.walk(_dir):
            for f in files:
                if '-' not in f: continue
                if f.endswith('.bz2'):
                    _f = BZ2File(os.path.join(dirpath,f))
                    f = f[:-4]
                else:
                    _f = open(os.path.join(dirpath,f))
                builder = dirpath.rsplit("/", 1)[-1:]
                logname = f
                buildnumber, _log, step, name = logname.split("-")
                del chunks[:]
                ls.dataReceived(_f.read())
                for chunk in chunks:
                    conn.index(chunk, "l10n-master", "logs", bulk=True)
                conn.index({"log_finished":True, "master":"l10n-master",
                            "builder":builder, "buildnumber": buildnumber,
                            "step": step, "logname": logname, "name": name}, "l10n-master", "logs", bulk=True)
                sys.stdout.write('.')
                sys.stdout.flush()
    conn.force_bulk()
    print

main()
