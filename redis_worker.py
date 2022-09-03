from redisTool import RedisQueue
import json

q = RedisQueue('Nuevos')

while not q.empty():
    trabajo = json.loads(q.get().decode('UTF-8'))
    print(trabajo["nombre"])
