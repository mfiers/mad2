


from mad2.store.sidecar import SidecarStore
from mad2.store.mongo import MongoStore

all_stores = {
    'sidecar' : SidecarStore,
    'mongo' : MongoStore,
}
