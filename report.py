#!/usr/bin/python

from keystoneauth1.identity import v3
from keystoneauth1 import session
from gnocchiclient.client import Client
from datetime import datetime

import rados, rbd

auth = v3.Password(auth_url='http://127.0.0.1:5000/v3',
                   username='ceph-reporter',
                   password='secrete',
                   project_name='service',
                   user_domain_name='Default',
                   project_domain_name='Default')
sess = session.Session(auth=auth)
client = Client(1, session=sess)

volume_pools = ['volumes']
metric_batch = dict()
raw_data = dict()
class StateTracker(object):
    def __init__(self):
        self.extent_size_list = []
    def _add_extent(self, size):
        self.extent_size_list.append(size)
    def _size_callback(self, offset, length, exists):
        if exists:
            self._add_extent(length)
    @property
    def total_size(self):
        ret_val = 0
        for x in self.extent_size_list:
            ret_val += x
        return ret_val


def _get_image_list(pool_name, cluster):
    with cluster.open_ioctx(pool_name) as ioctx:
        rbd_inst = rbd.RBD()
        rbd_volumes = rbd_inst.list(ioctx)
    return rbd_volumes

def _get_image_used_size(pool_name, cluster, image):
    with cluster.open_ioctx(pool_name) as ioctx:
        image = rbd.Image(ioctx, image, read_only=True)
        tracker = StateTracker()
        image.diff_iterate(0, image.size(), None, tracker._size_callback, include_parent=False)
        return tracker.total_size

def _get_image_snapshot_size(pool_name, cluster, image):
    with cluster.open_ioctx(pool_name) as ioctx:
        image = rbd.Image(ioctx, image, read_only=True)
        snap_total = 0
        for snap in image.list_snaps():
            tracker = StateTracker()
            image.diff_iterate(0, image.size(), snap['name'], tracker._size_callback, include_parent=False)
            snap_total += tracker.total_size
        return snap_total

def gather():
    with rados.Rados(conffile='/etc/ceph/ceph.conf', clustername='ceph', rados_id='openstack') as cluster:
        for image in _get_image_list('volumes', cluster):
            size = _get_image_used_size('volumes', cluster, image)
            snap_size = _get_image_snapshot_size('volumes', cluster, image)
            vol_id = image.replace('volume-','')
            raw_data[vol_id] = dict(vol=size,snap=snap_size,ts=datetime.utcnow())

def build_metric_dictionary():
    for vol_id,data in raw_data.iteritems():
       metric_batch[vol_id] = {
               'volume.real_size': [{'timestamp': data['ts'], 'value': float(data['vol'])}],
               'snapshot.real_size': [{'timestamp': data['ts'], 'value': float(data['snap'])}]
               }

def upload():
    client.metric.batch_resources_metrics_measures(metric_batch, create_metrics=True)

if __name__ == '__main__':
    gather()
    build_metric_dictionary()
    upload()
