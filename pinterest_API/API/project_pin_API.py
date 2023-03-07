from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer
import json

app = FastAPI()

# producer = KafkaProducer(
#     bootstrap_servers='localhost:9092',
#     value_serializer= lambda x : str(x, 'utf-8')
# )

# producer = KafkaProducer(bootstrap_servers='localhost:9092',
# value_serializer=lambda v: json.dumps(v).encode('utf-8'))

producer = KafkaProducer(bootstrap_servers='localhost:9092',
value_serializer=lambda v: dumps(v).encode('ascii')
)

class Data(BaseModel):
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str


@app.post("/pin/")
def get_db_row(item: Data):
    batch = 0
    while batch <=50:
        data = dict(item)
        # print(data)
        # for k in data:
        #     producer.send('firstTopic', k)
        producer.send('batchtest', data)
        producer.send('streamTopic', data)
        batch += 1
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
