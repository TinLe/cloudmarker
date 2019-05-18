"""Microsoft Azure virtual machine plugin to read Azure virtual machine data.

This module defines the :class:`AzVM` class that retrieves virtula machine data
from Microsoft Azure.
"""


import logging

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.resource import SubscriptionClient
from msrestazure import tools

from cloudmarker import ioworkers, util

_log = logging.getLogger(__name__)


class AzVM:
    """Azure Virtual Machine plugin."""

    def __init__(self, tenant, client, secret, processes=4, threads=30,
                 _max_subs=0, _max_recs=0):
        """Create an instance of :class:`AzVM` plugin.

         Note: The ``_max_subs`` and ``_max_recs`` arguments should be
         used only in the development-test-debug phase. They should not
         be used in production environment. This is why we use the
         convention of beginning their names with underscore.

        Arguments:
            tenant (str): Azure subscription tenant ID.
            client (str): Azure service principal application ID.
            secret (str): Azure service principal password.
            _max_subs (int): Maximum number of subscriptions to fetch
                data for if the value is greater than 0.
            _max_recs (int): Maximum number of virtual machines records
                to fetch for each subscription.
        """
        self._credentials = ServicePrincipalCredentials(
            tenant=tenant,
            client_id=client,
            secret=secret,
        )
        self._tenant = tenant
        self._processes = processes
        self._threads = threads
        self._max_subs = _max_subs
        self._max_recs = _max_recs

    def read(self):
        """Return an Azure virtual machine record.

        Yields:
            dict: An Azure virtual machine record.

        """
        yield from ioworkers.run(self._get_vms,
                                 self._get_vm_instance_views,
                                 self._processes, self._threads)

    def _get_vms(self):
        """Generate tuples with compute client, VMs, and subscriptions.

        The yielded tuples when unpacked would become arguments for
        :meth:`_get_vm_instance_views`. Each such tuple represents a
        single unit of work that :meth:`_get_vm_instance_views` can work
        on independently in its own worker thread.

        Yields:
            tuple: A tuple which when unpacked forms valid arguments for
                :meth:`_get_vm_instance_views`.

        """
        try:
            tenant = self._tenant
            sub_client = SubscriptionClient(self._credentials)
            sub_list = sub_client.subscriptions.list()

            for sub_index, sub in enumerate(sub_list):
                _log.info('Found %s', util.outline_az_sub(sub_index,
                                                          sub, tenant))

                compute_client = ComputeManagementClient(self._credentials,
                                                         sub.subscription_id)
                vm_list = compute_client.virtual_machines.list_all()

                for vm_index, vm in enumerate(vm_list):
                    _log.info('Found VM #%d: %s; %s', vm_index, vm.name,
                              util.outline_az_sub(sub_index, sub, tenant))

                    # Each VM is a unit of work.
                    yield (vm_index, vm, sub_index, sub)

                    # Break after pulling data for self._max_recs number
                    # of VMs for a subscriber. Note that if
                    # self._max_recs is 0 or less, then the following
                    # condition never evaluates to True.
                    if vm_index + 1 == self._max_recs:
                        _log.info('Stopping vm_instance_view fetch due '
                                  'to _max_recs: %d; %s', self._max_recs,
                                  util.outline_az_sub(sub_index, sub, tenant))
                        break

                # Break after pulling data for self._max_subs number of
                # subscriptions. Note that if self._max_subs is 0 or less,
                # then the following condition never evaluates to True.
                if sub_index + 1 == self._max_subs:
                    _log.info('Stopping subscriptions fetch due to '
                              '_max_subs: %d; tenant: %s', self._max_subs,
                              tenant)
                    break

        except Exception as e:
            _log.error('Failed to fetch VMs; %s; error: %s: %s',
                       util.outline_az_sub(sub_index, sub, tenant),
                       type(e).__name__, e)

    def _get_vm_instance_views(self, vm_index, vm, sub_index, sub):
        """Get virtual machine records with instance view details.

        Arguments:
            vm_index (int): Virtual machine index (for logging only).
            vm (VirtualMachine): Virtual machine object.
            sub_index (int): Subscription index (for logging only).
            sub (Subscription): Azure subscription object.

        Yields:
            dict: An Azure virtual machine record with instance view details.

        """
        _log.info('Working on VM #%d: %s; %s', vm_index, vm.name,
                  util.outline_az_sub(sub_index, sub, self._tenant))
        try:
            compute_client = ComputeManagementClient(self._credentials,
                                                     sub.subscription_id)
            rg_name = tools.parse_resource_id(vm.id)['resource_group']
            vm_iv = compute_client.virtual_machines.instance_view(rg_name,
                                                                  vm.name)
            yield _process_vm_instance_view(vm_index, vm, vm_iv,
                                            sub_index, sub, self._tenant)
        except Exception as e:
            _log.error('Failed to fetch vm_instance_view for VM #%d: '
                       '%s; %s; error: %s: %s', vm_index, vm.name,
                       util.outline_az_sub(sub_index, sub, self._tenant),
                       type(e).__name__, e)

    def done(self):
        """Perform clean up tasks.

        Currently, this method does nothing because there are no clean
        up tasks associated with the :class:`AzVM` plugin. This
        may change in future.
        """


def _process_vm_instance_view(vm_index, vm, vm_iv,
                              sub_index, sub, tenant):
    """Process virtual machine record and yeild them.

    Arguments:
        vm_index (int): Virtual machine index (for logging only).
        vm (VirtualMachine): Virtual Machine Descriptor
        vm_iv (VirtualMachineInstanceView): Virtual Machine Instance view
        sub_index (int): Subscription index (for logging only).
        sub (Subscription): Azure subscription object.
        tenant (str): Azure tenant ID.

    Yields:
        dict: An Azure record of type ``vm_instance_view``.

    """
    raw_record = vm.as_dict()
    raw_record['instance_view'] = vm_iv.as_dict()
    record = {
        'raw': raw_record,
        'ext': {
            'cloud_type': 'azure',
            'record_type': 'vm_instance_view',
            'subscription_id': sub.subscription_id,
            'subscription_name': sub.display_name,
            'subscription_state': sub.state.value,
        },
        'com': {
            'cloud_type': 'azure',
            'record_type': 'compute',
            'reference': raw_record.get('id')
        }
    }
    record['ext'] = util.merge_dicts(
        record['ext'],
        _get_normalized_vm_statuses(vm_iv),
        _get_normalized_vm_disk_encryption_status(vm, vm_iv)
        )
    _log.info('Found vm_instance_view #%d: %s; %s',
              vm_index, raw_record.get('name'),
              util.outline_az_sub(sub_index, sub, tenant))
    return record


def _get_normalized_vm_statuses(vm_iv):
    """Iterate over a list of virtual machine statuses and normalize them.

    Arguments:
        vm_iv (VirtualMachineInstanceView): Virtual Machine Instance View

    Returns:
        dict: Normalized virtual machine statuses

    """
    normalized_statuses = {}
    for s in vm_iv.statuses:
        if s.code.startswith('PowerState/'):
            code_elements = s.code.split('/', 1)
            normalized_statuses['power_state'] = \
                code_elements[1].lower()
    return normalized_statuses


def _get_normalized_vm_disk_encryption_status(vm, vm_iv):
    """Iterate over a list of virtual machine disks normalize them.

    Arguments:
        vm (VirtualMachine): Virtual Machine
        vm_iv (VirtualMachineInstanceView): Virtual Machine Instance View

    Returns:
        dict: Normalized virtual machine disk encryption statuses

    """
    os_disk_name = vm.storage_profile.os_disk.name
    disk_enc_statuses = {}
    for disk in vm_iv.disks:
        if disk.name == os_disk_name:
            if disk.encryption_settings is None:
                disk_enc_statuses['os_disk_encrypted'] = False
            else:
                disk_enc_statuses['os_disk_encrypted'] = \
                    disk.encryption_settings[0].enabled
        else:
            if disk_enc_statuses.get('all_data_disks_encrypted', True):
                if disk.encryption_settings is None:
                    disk_enc_statuses['all_data_disks_encrypted'] = False
                else:
                    disk_enc_statuses['all_data_disks_encrypted'] = \
                        disk.encryption_settings[0].enabled
    return disk_enc_statuses
