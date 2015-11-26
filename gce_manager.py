import logging
import sys
import time

from oauth2client.client import GoogleCredentials
from googleapiclient import discovery

class GCEManagerError(Exception):
    pass

class GCEManager(object):
    def __init__(self,
                 zone="us-central1-f",
                 project="default",
                 disk_image="image",
                 machine_type="n1-standard-1",
                 number_of_instances=1):

        self.logger = logging.getLogger("GCEManager")
        self.logger.setLevel(logging.DEBUG)

        # Print simplified log to stdout
        formatter = logging.Formatter("%(asctime)s : %(message)s")
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        # Save a more detailed log to file
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.FileHandler("GCEManager.log", mode = 'w')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.logger.info("Initializing GCEManager.")

        gce_logger = logging.getLogger("googleapiclient.discovery")
        oauth2_logger = logging.getLogger("oauth2client.client")

        gce_logger.setLevel(logging.WARNING)
        oauth2_logger.setLevel(logging.WARNING)

        self.zone = zone
        self.project = project
        self.disk_image = disk_image
        self.machine_type = machine_type
        self.number_of_instances = number_of_instances

        self.credentials = GoogleCredentials.get_application_default()
        self.compute = discovery.build('compute', 'v1', credentials=self.credentials)
        
        self.instances = None

        self.logger.debug("Using the following configuration: "
                "disk_image = {0}, machine_type = {1}, zone = {2}, project = {3}"
                .format(disk_image, machine_type, zone, project))

        self.logger.info("Initialization complete.")

    def wait_for_operation(self, operations):
        self.logger.info("Waiting for operations to finish...")
        while True:
            results = [self.compute.zoneOperations().get(
                project   = self.project,
                zone      = self.zone,
                operation = operation).execute() for operation in operations ]

            if all(result['status'] == 'DONE' for result in results):
                self.logger.info("Done.")
                for result in results:
                    if 'error' in result:
                        raise Exception(result['error'])
                return result
            else:
                time.sleep(1)

    def list_instances(self):
        result = self.compute.instances().list(project=self.project, zone=self.zone).execute()

        try:
            return result['items']
        except KeyError as err:
            raise GCEManagerError("no instances available in project {0}, zone {1}"
                    .format(self.project, self.zone)) from None

    def create_instance(self, name):
        disk_image_url = "projects/{0}/global/images/{1}".format(self.project, self.disk_image)
        machine_type_url = "zones/{0}/machineTypes/{1}".format(self.zone, self.machine_type)
        with open('startup-script.sh', 'r') as f:
            startup_script = f.read()

        config = {
            'name': name,
            'machineType': machine_type_url,
            # Specify the boot disk and the image to use as a source.
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': disk_image_url,
                    }
                }
            ],
            # Specify a network interface with NAT to access the public
            # internet.
            'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],
            # Allow the instance to access cloud storage and logging.
            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_write',
                    'https://www.googleapis.com/auth/logging.write'
                ]
            }],
            # Metadata is readable from the instance and allows you to
            # pass configuration from deployment scripts to instances.
            'metadata': {
                'items': [{
                    # Startup script is automatically executed by the
                    # instance upon startup.
                    'key': 'startup-script',
                    'value': startup_script
                }, {
                    # Every project has a default Cloud Storage bucket that's
                    # the same name as the project.
                    'key': 'bucket',
                    'value': self.project
                }]
            }
        }
        return self.compute.instances().insert(
            project = self.project,
            zone    = self.zone,
            body    = config).execute()

    def create_all(self):
        self.logger.info("Creating instances.")
        operations = []

        for i in range(self.number_of_instances):
            instance_name = "{0}-{1}".format(self.project, i)
            operations.append(self.create_instance(instance_name)['name'])
            self.logger.debug("Creating {0}.".format(instance_name))

        self.wait_for_operation(operations)
        self.instances = self.list_instances()

    def delete_instance(self, name):
        return self.compute.instances().delete(
            project  = self.project,
            zone     = self.zone,
            instance = name).execute()

    def delete_all(self):
        self.logger.info("Deleting all instances.")
        operations = []

        for instance in self.instances:
            operations.append(self.delete_instance(instance['name'])['name'])
            self.logger.debug("Deleting {0}.".format(instance['name']))

        self.wait_for_operation(operations)


if __name__ == "__main__":
    gce_man = GCEManager(zone="us-central1-f", project="brave-set-92418", disk_image="bsptest")
    gce_man.create_all()
    gce_man.delete_all()
