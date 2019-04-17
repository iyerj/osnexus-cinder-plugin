# Copyright 2018 Quantastor
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


from oslo_concurrency import processutils
from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import units

from cinder import exception
from cinder.i18n import _
from cinder import interface

from cinder.volume.drivers import quantastor_api
from cinder.volume.drivers.san import san

LOG = logging.getLogger(__name__)

QS_OPTS = [
    cfg.StrOpt('qs_pool_id',
               default=None,
               help='The ID of the Quantastor pool or tier to use.')
]

CONF = cfg.CONF
CONF.register_opts(QS_OPTS)


class DotDict(dict):
    """dot.notation access to dictionary attributes"""
    __getattr__ = dict.__getitem__
    __setattr__ = dict.__setitem__
    __delattr__ = dict.__delitem__


@interface.volumedriver
class QuantaStorDriver(san.SanDriver):
    """Executes commands relating to QuantaStor Volumes"""
    VERSION = "1.0.0"

    # ThirdPartySystems wiki page
    CI_WIKI_NAME = "Quantastor CI"

    def __init__(self, *args, **kwargs):
        super(QuantaStorDriver, self).__init__(*args, **kwargs)
        self.configuration.append_config_values(QS_OPTS)

    def do_setup(self, context):
        """Create client for QUANTASTOR request manager."""
        LOG.debug("Enter in QuantastorDriver do_setup.")

        required_config = ['san_ip', 'san_login', 'san_password']
        for attr in required_config:
            if not getattr(self.configuration, attr, None):
                raise exception.InvalidInput(reason=_('%s is not set.') %
                                             attr)
        module = {
            'params': {
                'qs_hostname': self.configuration.san_ip,
                'qs_username': self.configuration.san_login,
                'qs_password': self.configuration.san_password
            }}
        module = DotDict(module)
        self.client = quantastor_api.QuantastorClient(
            module, self.configuration.driver_ssl_cert_verify)

    def check_for_setup_error(self):
        """Returns an error if prerequisites aren't met"""
        try:
            LOG.info("Checking for setup errors...")

            sys_info = self.qs_sys_get()
            pool_info = self.qs_pool_get()
            LOG.debug("connected to Quantastor system =%s Found pool with "
                      "ID =%s named %s ", sys_info._name, pool_info._id,
                      pool_info._name)

        except processutils.ProcessExecutionError:
            exception_message = _("Quantastor driver is not working.")
            raise exception.VolumeBackendAPIException(data=exception_message)

    def create_cloned_volume(self, volume, src_vref):
        out = self.client.storage_create_cloned_volume(src_vref['name'],
                                                       volume['name'],
                                                       self.configuration
                                                       .qs_pool_id)
        if volume['size'] > src_vref['size']:
            self.extend_volume(volume, volume['size'])

        qs_clone = out._id
        if qs_clone is None:
            exception_message = _(
                "Error: Failed to load clone information")
            raise exception.VolumeBackendAPIException(data=exception_message)

        LOG.debug("Cloned volume %s was successfully created", qs_clone)

    def create_volume(self, volume):
        """Creates a volume"""
        name = volume['name']
        size = self.gb_to_b_size(volume['size'])
        description = 'volume creation'
        provisionable_id = self.configuration.qs_pool_id
        out = self.client.storage_volume_create(name, size,
                                                description,
                                                provisionable_id)
        qs_vol = out._id
        if qs_vol is None:
            exception_message = _(
                "Error: Failed to load volume information")
            raise exception.VolumeBackendAPIException(data=exception_message)
        LOG.debug("Volume = %s was successfully created", qs_vol)

    def create_volume_from_snapshot(self, volume, snapshot):
        """Creates a volume from a snapshot."""
        out = self.client.storage_create_cloned_volume(snapshot['name'],
                                                       volume['name'],
                                                       self.configuration
                                                       .qs_pool_id)
        if volume['size'] > snapshot['volume_size']:
            self.extend_volume(volume, volume['size'])
        qs_clone = out._id
        if qs_clone is None:
            exception_message = _(
                "Error: Failed to load clone information")
            raise exception.VolumeBackendAPIException(data=exception_message)

        LOG.debug("Cloned volume %s was successfully created", qs_clone)

    def delete_volume(self, volume):
        """Deletes a logical volume"""
        name = volume['name']
        LOG.debug("deleting a volume")
        self.client.storage_volume_delete(name)

        LOG.debug("verifying that volume no longer exists")
        vol = self.client.storage_volume_get(name)
        if vol is not None:
            exception_message = _("Error: Failed to delete volume")
            raise exception.VolumeBackendAPIException(data=exception_message)
        LOG.debug("Volume =%s was successfully deleted", vol)

    def create_snapshot(self, snapshot):
        storage_volume = snapshot['volume_name']
        snapshot_name = snapshot['name']
        out = self.client.storage_create_snapshot(storage_volume,
                                                  snapshot_name,
                                                  self.configuration
                                                  .qs_pool_id)

        if out._id is None:
            exception_message = _(
                "Error: Failed to load snapshot information")
            raise exception.VolumeBackendAPIException(data=exception_message)

        LOG.debug("Snapshot =%s was successfully created", snapshot_name)

    def delete_snapshot(self, snapshot):
        """Deletes a snapshot"""
        LOG.debug("deleting a snapshot")
        name = snapshot['name']
        self.client.storage_volume_delete(name)

        LOG.debug("verifying that snapshot no longer exists")
        out = self.client.storage_volume_get(name)

        if out and out._id is not None:
            exception_message = _("Error: Failed to delete snapshot")
            raise exception.VolumeBackendAPIException(data=exception_message)

        LOG.debug("Snapshot =%s was successfully deleted", name)

    def ensure_export(self, context, volume):
        """Safely and synchronously recreates an export for a logical volume"""

    def remove_export(self, context, volume):
        """Removes an export for a logical volume"""

    def initialize_connection(self, volume, connector):
        """attaching volume to host"""
        initiator = connector["initiator"]
        host_name = "ostack-" + initiator[initiator.rfind(":") + 1:]
        volume_name = volume["name"]

        # Get the volume from the system to verify that it still exists
        name = volume_name
        qs_vol = self.client.storage_volume_get(name)
        if qs_vol is None or qs_vol._id is None:
            exception_message = _(
                "Error: Failed to load volume information")
            raise exception.VolumeBackendAPIException(data=exception_message)

        # verify that there is a host entry for the specified iqn
        out = self.client.host_get(initiator)
        try:
            host_obj = out._id
        except BaseException:
            LOG.debug("Host with specified iqn not found. Creating "
                      "new host")
            out = self.client.host_add(host_name, initiator)
            host_obj = out._id
            if host_obj is None:
                exception_message = _("Error: Failed to create "
                                      "host entry in QuantaStor")
                raise exception.VolumeBackendAPIException(
                    data=exception_message)
        LOG.debug("Attaching volume to host")
        response = self.client.storage_volume_attach(volume_name, host_obj)

        # verify that it was added successfully
        acl = response._hostId
        if acl is None:
            exception_message = _(
                "Error: Failed to assign volume to host")
            raise exception.VolumeBackendAPIException(data=exception_message)

        host_ip = self.client.build_tgt_ip(self.configuration.san_ip)
        portal = host_ip + ":3260"

        result = {
            'driver_volume_type': 'iscsi',
            'data': {
                'target_discovered': True,
                'target_iqn': str(qs_vol._iqn),
                'target_portal': portal,
                'volume_id': volume['id'],
                'access_mode': 'rw'
            }
        }
        LOG.debug("Successfully attached volume to host")
        return result

    def terminate_connection(self, volume, connector, **kwargs):
        """Terminating connection"""
        # get the storage volume information
        vol_info = self.qs_vol_get(volume["name"])
        if vol_info._id is None:
            LOG.warning("Warning: Unable to find volume")
            return
        vol_id = vol_info._id

        if connector is None:
            LOG.warning("Removing ALL host connections for volume %s",
                        vol_id)
            acl_list = self.client.storage_volume_acl_list(vol_id)
            if acl_list is not None:
                for acl in acl_list:
                    self.client.storage_volume_dettach(vol_id, acl)
            return

        initiator = connector["initiator"]

        # remove the acl assignment for the volume/host
        self.client.storage_volume_dettach(vol_id, initiator)

    def _update_volume_stats(self):
        stats = {}

        stats["volume_backend_name"] = self.configuration.volume_backend_name
        stats['vendor_name'] = 'OSNEXUS'
        stats['driver_version'] = self.VERSION
        stats['storage_protocol'] = 'iSCSI'
        stats['total_capacity_gb'] = 'unknown'
        stats['free_capacity_gb'] = 'unknown'
        stats['reserved_percentage'] = 0
        stats['QoS_support'] = False

        # this gives the stats for the pool being used in QuantaStor
        pool = self.qs_pool_get()
        if pool._size is not None:
            stats['total_capacity_gb'] = self.b_to_gb_size(pool._size)
        if pool._freeSpace is not None:
            stats['free_capacity_gb'] = self.b_to_gb_size(pool._freeSpace)

        self._stats = stats

    def get_volume_stats(self, refresh=False):
        if refresh:
            self._update_volume_stats()
        return self._stats

    def extend_volume(self, volume, new_size):
        """Extend an Existing Volume."""
        LOG.debug("Extending volume")
        out = self.client.storage_extend_volume(volume['name'],
                                                self.configuration.qs_pool_id,
                                                self.gb_to_b_size(new_size))
        qs_vol = out._id
        qs_size = out._size
        if qs_vol is None:
            exception_message = _(
                "Error: Failed to get volume information")
            raise exception.VolumeBackendAPIException(data=exception_message)
        if int(qs_size) != int(self.gb_to_b_size(new_size)):
            exception_message = _("Error: Failed to extend volume")
            raise exception.VolumeBackendAPIException(data=exception_message)

        LOG.debug("Volume was successfully extended to %s", qs_size)

    @classmethod
    def gb_to_b_size(cls, size_gb):
        """converting gb to b"""
        return str(int(size_gb) * (units.Ki ** 3))

    @classmethod
    def b_to_gb_size(cls, size_b):
        """converting b to gb"""
        return float(int(size_b) / (units.Ki ** 3))

    def qs_vol_get(self, vol_name):
        """volume info"""
        vol = self.client.storage_volume_get(vol_name)
        if vol is None or vol._id is None:
            exception_message = _("Error: Failed to load volume information")
            raise exception.VolumeBackendAPIException(data=exception_message)
        return vol

    def qs_pool_get(self):
        """pool info"""
        pool = self.client.storage_pool_get(self.configuration.qs_pool_id)
        if pool is None or pool._id is None:
            exception_message = _(
                "Error: Failed to load QuantaStor pool information")
            raise exception.VolumeBackendAPIException(data=exception_message)
        return pool

    def qs_sys_get(self):
        """system info"""
        sys = self.client.storage_system_noflag_get()
        if sys is None or sys._id is None:
            exception_message = _(
                "Error: Failed to load QuantaStor system information")
            raise exception.VolumeBackendAPIException(data=exception_message)
        return sys
