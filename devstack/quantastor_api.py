# Copyright 2018 QuantaStor
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.


import eventlet
import ipaddress
import requests

from cinder import exception
from cinder.i18n import _
from cinder import utils


class QuantastorAPIException(exception.VolumeBackendAPIException):
    message = _("Unexpected response from Quantastor API")


class QuantastorClient(object):
    """QuantaStor client class to make request calls """
    _auth = None
    _osnexusUrl = ""
    _module = None

    def __init__(self, module, verify):
        self._module = module

        qs_hostname = self.build_tgt_ip(module.params['qs_hostname'])
        qs_username = module.params['qs_username']
        qs_password = module.params['qs_password']
        self.verify = verify
        self._auth = (qs_username, qs_password)
        self._base_url = "https://" + qs_hostname + ":8153/qstorapi/"

    def make_call(self, api, payload):
        """makes http request"""
        str_url = self._base_url + api
        r = requests.get(str_url, params=payload,
                         verify=self.verify, auth=self._auth)
        if r.status_code != 200:
            msg = _("Failed to make a request "
                    "%(api)s : %(payload)s : %(code)s") % {
                'api': api,
                'payload': payload,
                'code': r.status_code}
            raise exception.VolumeDriverException(msg)

        json_output = r.json()
        if isinstance(json_output, dict) and "RestError" in json_output:
            msg = _("Failed to execute api "
                    "%(api)s : %(payload)s : %(code)s") % {
                'api': api,
                'payload': payload,
                'code': json_output['RestError']}
            raise exception.VolumeDriverException(msg)

        return json_output

    @utils.retry(QuantastorAPIException, interval=2, retries=3)
    def wait_on_task(self, json_output):
        """waits for the task"""
        if 'task' not in json_output.keys():
            raise Exception("Task object not found in jsonOutput")
        task = json_output["task"]
        task_id = task["id"]

        i = 0
        while True:
            i = i + 1
            payload = {'id': task_id}
            json_output = self.make_call("taskGet", payload)
            if 'taskState' in json_output.keys():
                task_state = json_output["taskState"]
                if task_state == 5:  # OSN_TASKSTATE_COMPLETED
                    return json_output["customId"]
                elif task_state == 4:  # OSN_TASKSTATE_CANCELLED
                    msg = _("ERROR: Task %(task_id)s cancelled "
                            "at state : %(json_output)s") % {
                        'task_id': task_id,
                        'json_output': json_output["description"]}
                    raise QuantastorAPIException(msg)

                elif task_state == 3:  # OSN_TASKSTATE_FAILED
                    msg = _("ERROR: Task %(task_id)s failed "
                            "with error : %(json_output)s") % {
                        'task_id': task_id,
                        'json_output': json_output["description"]}
                    raise QuantastorAPIException(msg)
            if i == 10:
                break
        raise Exception("Task %s did not complete" % task_id)

    # Storage System APIs
    def storage_system_noflag_get(self):
        payload = {'flags': 0}
        json_output = self.make_call("storageSystemGet", payload)
        if 'name' in json_output.keys() and 'id' in json_output.keys():
            system = StorageSystem(json_output["name"], json_output["id"])
            return system
        raise Exception("Failed to gather QuantaStor storage system "
                        "information for system with ID '%s'."
                        % json_output.id)

    # Host management APIs

    def host_parse(self, json_output):
        if 'obj' in json_output.keys() and 'id' in json_output['obj'].keys():
            json_output = json_output['obj']
        initiator_port_list = []
        if 'initiatorPortList' in json_output:
            port_list = json_output["initiatorPortList"]
            for port in port_list:
                if 'iqn' in port and port['iqn']:
                    initiator_port_list.append(port['iqn'])
                elif 'wwpn' in port and port['wwpn']:
                    initiator_port_list.append(port['wwpn'])
        return Host(json_output["name"],
                    json_output["id"], initiator_port_list)

    def host_add(self, hostname, host_iqn):
        payload = {'hostname': hostname, 'iqn': host_iqn,
                   'hostType': 3, 'flags': 1}
        json_output = self.make_call("hostAdd", payload)
        eventlet.sleep(2)
        if (json_output and json_output['task'] and
                'id' in json_output['task'].keys()):
            self.wait_on_task(json_output)
        return self.host_get(hostname)

    def host_remove(self, hostname):
        payload = {'host': hostname, 'flags': 1}
        json_output = self.make_call("hostRemove", payload)
        if json_output['obj'] and 'id' in json_output['obj'].keys():
            return self.host_parse(json_output['obj'])

    def host_get(self, host_id):
        payload = {'host': host_id, 'flags': 0}
        try:
            json_output = self.make_call("hostGet", payload)
            if json_output:
                return self.host_parse(json_output)
        except BaseException:
            pass

    def host_initiator_get(self, host_iqn):
        payload = {'initiator': host_iqn}
        json_output = self.make_call("hostInitiatorGet", payload)
        if 'obj' in json_output.keys():
            host_obj = json_output["obj"]
            host = Host(host_obj["hostId"], host_obj["hostName"],
                        host_obj["list"])
            return host

    def host_initiator_add(self, host_id, host_iqn):
        payload = {'host': host_id, 'iqn': host_iqn}
        json_output = self.make_call("hostInitiatorAdd", payload)
        if json_output:
            return self.host_parse(json_output)

    def host_initiator_remove(self, host_id, host_iqn):
        payload = {'host': host_id, 'iqn': host_iqn}
        json_output = self.make_call("hostInitiatorRemove", payload)
        if json_output:
            return self.host_parse(json_output)

    # Host Group management APIs

    def host_group_parse(self, json_output):
        if 'obj' in json_output.keys() and 'id' in json_output['obj'].keys():
            json_output = json_output['obj']
        host_list = []
        if 'hostList' in json_output:
            host_obj_list = json_output["hostList"]
            for host in host_obj_list:
                host_list.append(self.host_parse(host))
        return HostGroup(json_output["name"], json_output["id"], host_list)

    def host_group_get(self, host_group):
        payload = {'hostGroup': host_group}
        try:
            json_output = self.make_call("hostGroupGet", payload)
            if json_output:
                return self.host_group_parse(json_output)
        except BaseException:
            pass

    def host_group_create(self, name, host_list):
        payload = {'name': name, 'hostList': host_list, 'flags': 1}
        json_output = self.make_call("hostGroupCreate", payload)
        if (json_output and json_output['task'] and
                'id' in json_output['task'].keys()):
            self.wait_on_task(json_output)
        return self.host_group_get(name)

    def host_group_delete(self, name):
        payload = {'host': name, 'flags': 1}
        json_output = self.make_call("hostGroupDelete", payload)
        if json_output:
            if json_output['obj'] and 'id' in json_output['obj'].keys():
                return self.host_group_parse(json_output['obj'])

    # Storage Volume management APIs

    def storage_volume_parse(self, json_output):
        if 'obj' in json_output.keys() and 'id' in json_output['obj'].keys():
            obj = json_output['obj']
            return Volume(obj['name'], obj['id'], obj['size'], obj['iqn'])
        return Volume(json_output['name'], json_output['id'],
                      json_output['size'], json_output['iqn'])

    def storage_volume_list(self):
        payload = {}
        json_output = self.make_call('storageVolumeEnum', payload)
        volumes = []
        for line in json_output:
            vol = Volume(line["name"], line["id"], line["size"], line["iqn"])
            volumes.append(vol)
        return volumes

    def storage_volume_create(self, name, size, description, provisionable_id):
        payload = {'count': 1,
                   'name': name,
                   'description': description,
                   'accessMode': 0,
                   'flags': 1,
                   'thinProvisioned': True,
                   'size': str(size),
                   'provisionableId': provisionable_id}
        json_output = self.make_call('storageVolumeCreate', payload)
        eventlet.sleep(3)
        custom_id = self.wait_on_task(json_output)
        return self.storage_volume_get(custom_id)

    def storage_create_cloned_volume(self, src_vref, volume, provisionable_id):
        payload = {'storageVolume': src_vref,
                   'cloneName': volume,
                   'accessMode': 0,
                   'flags': 0,
                   'provisionableId': provisionable_id}
        json_output = self.make_call('storageVolumeClone', payload)
        eventlet.sleep(3)
        custom_id = self.wait_on_task(json_output)
        return self.storage_volume_get(custom_id)

    def storage_create_snapshot(self, storage_volume, snapshot_name,
                                provisionable_id):
        payload = {'storageVolume': storage_volume,
                   'snapshotName': snapshot_name,
                   'accessMode': 0,
                   'flags': 0,
                   'provisionableId': provisionable_id}
        json_output = self.make_call('storageVolumeSnapshot', payload)
        eventlet.sleep(2)
        custom_id = self.wait_on_task(json_output)
        return self.storage_volume_get(custom_id)

    def storage_extend_volume(self, volume_name, provisionable_id, size):
        payload = {'storageVolume': volume_name,
                   'provisionableId': provisionable_id,
                   'newSizeInBytes': size}
        json_output = self.make_call('storageVolumeResize', payload)
        eventlet.sleep(3)
        custom_id = self.wait_on_task(json_output)
        return self.storage_volume_get(custom_id)

    def storage_volume_delete(self, id):
        payload = {'storageVolume': id,
                   'flags': 3}
        json_output = self.make_call('storageVolumeDeleteEx', payload)
        eventlet.sleep(3)
        self.wait_on_task(json_output)

    def storage_volume_get(self, volume):
        payload = {'storageVolume': volume}
        try:
            json_output = self.make_call("storageVolumeGet", payload)
            if json_output:
                if ('obj' in json_output.keys() and
                        'id' in json_output['obj'].keys()):
                    obj = json_output['obj']
                    return self.storage_volume_parse(obj)
                elif ('name' in json_output.keys() and
                        'id' in json_output.keys()):
                    return self.storage_volume_parse(json_output)
        except BaseException:
            pass

    # Storage Volume ACL assignement management APIs

    def storage_volume_acl_get(self, volume, host):
        try:
            payload = {'storageVolume': volume,
                       'host': host}
            json_output = self.make_call("storageVolumeAclGet", payload)
            if json_output:
                return VolumeAcl(json_output['storageVolumeId'],
                                 json_output["hostId"])
        except BaseException:
            pass

    def storage_volume_acl_list(self, volume):
        payload = {'storageVolume': volume}
        json_output = self.make_call("storageVolumeAclEnum", payload)
        acl_list = []
        if json_output != 0:
            for acl in json_output:
                acl = VolumeAcl(volume, acl["hostId"])
                acl_list.append(acl)
        return acl_list

    def storage_volume_attach(self, host_id, host):
        payload = {'storageVolume': host_id,
                   'modType': 0,  # OSN_CMN_MOD_OP_ADD
                   'hostList': host,
                   'flags': 1}
        json_output = self.make_call("storageVolumeAclAddRemove", payload)
        eventlet.sleep(3)
        self.wait_on_task(json_output)
        return self.storage_volume_acl_get(host_id, host)

    def storage_volume_dettach(self, host_id, host):
        payload = {'storageVolume': host_id,
                   'modType': 1,  # OSN_CMN_MOD_OP_REMOVE
                   'hostList': host,
                   'flags': 1}
        json_output = self.make_call("storageVolumeAclAddRemove", payload)
        eventlet.sleep(3)
        self.wait_on_task(json_output)

    # Storage Pool management APIs

    def storage_pool_get(self, name):
        payload = {'storagePool': name}
        json_output = self.make_call('storagePoolGet', payload)
        pool = Pool(json_output['name'], json_output['id'],
                    json_output['freeSpace'], json_output['size'])
        return pool

    # Storage Tier management APIs

    def storage_tier_get(self, name):
        payload = {'storageTier': name}
        json_output = self.make_call('storageTierGet', payload)
        if 'obj' in json_output.keys():
            tier_obj = json_output["obj"]
            return Tier(tier_obj['name'], tier_obj['id'])

    @staticmethod
    def build_tgt_ip(ip_addr):
        try:
            addr = ipaddress.ip_address(u"%s" % ip_addr)
            if addr.version == 4:
                ip = ip_addr
            else:
                ip = '[%s]' % ip_addr
        except BaseException:
            raise Exception('san_ip address is invalid: ', ip_addr)
        return ip


class Tier(object):
    def __init__(self, name, id):
        self._name = name
        self._id = id


class Pool(object):
    def __init__(self, name, id, free_space, size):
        self._name = name
        self._id = id
        self._freeSpace = free_space
        self._size = size


class VolumeAcl(object):
    def __init__(self, storage_volume_id, host_id):
        self._storageVolumeId = storage_volume_id
        self._hostId = host_id


class StorageSystem(object):
    def __init__(self, name, id):
        self._name = name
        self._id = id


class Volume(object):
    def __init__(self, name, id, size, iqn):
        self._name = name
        self._id = id
        self._size = size
        self._iqn = iqn


class Host(object):
    def __init__(self, name, host_id, initiator_list):
        self._name = name
        self._id = host_id
        self._initiators = initiator_list


class HostGroup(object):
    def __init__(self, name, host_id, host_list):
        self._name = name
        self._id = host_id
        self._hosts = host_list
