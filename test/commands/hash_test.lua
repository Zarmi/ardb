--[[   --]]
ardb.call("del", "myhash")
local s = ardb.call("hmset", "myhash", "f0", "v0", "f1", "v1")
ardb.assert2(s["ok"] == "OK", s)
s = ardb.call("hmset2", "myhash", "f2", "v2", "f3", "v3")
ardb.assert2(s["ok"] == "OK", s)
local vs = ardb.call("hgetall", "myhash")
ardb.assert2(vs[1] == "f0", vs)
ardb.assert2(vs[2] == "v0", vs)
ardb.assert2(vs[3] == "f1", vs)
ardb.assert2(vs[4] == "v1", vs)
ardb.assert2(vs[5] == "f2", vs)
ardb.assert2(vs[6] == "v2", vs)
ardb.assert2(vs[7] == "f3", vs)
ardb.assert2(vs[8] == "v3", vs)
s = ardb.call("hlen", "myhash")
ardb.assert2(s == 4, vs)
vs = ardb.call("hkeys", "myhash")
ardb.assert2(vs[1] == "f0", vs)
ardb.assert2(vs[2] == "f1", vs)
ardb.assert2(vs[3] == "f2", vs)
ardb.assert2(vs[4] == "f3", vs)
vs = ardb.call("hvals", "myhash")
ardb.assert2(vs[1] == "v0", vs)
ardb.assert2(vs[2] == "v1", vs)
ardb.assert2(vs[3] == "v2", vs)
ardb.assert2(vs[4] == "v3", vs)

ardb.call("del", "myhash")
s = ardb.call("hset", "myhash", "f0", "v100")