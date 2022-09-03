from redisTool import RedisQueue

import json

q = RedisQueue('Nuevos')

dict = {"nombre":"paco","edad":25,"contador":"ss"}

for i in range(10):
    dict["contador"] = i
    q.put(json.dumps(dict))
