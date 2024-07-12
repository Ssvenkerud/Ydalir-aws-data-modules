from aws_cdk import Stack
from aws_cdk import aws_ec2 as _ec2
from constructs import Construct


class GlobalArgs:
    """
    Helper to define global statics
    """

    OWNER = ""
    ENVIRONMENT = ""
    REPO_NAME = ""
    SOURCE_INFO = f""
    VERSION = ""


class VpcStack(Stack):

    def __init__(
        self,
        scope: Construct,
        id: str,
        stack_log_level: str,
        from_vpc_name=None,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        if from_vpc_name is not None:
            self.vpc = _ec2.Vpc.from_lookup(self, "vpc", vpc_name=from_vpc_name)
        else:
            self.vpc = _ec2.Vpc(
                self,
                "vpcForDemo",
                ip_addresses=_ec2.IpAddresses.cidr("10.0.0.0/16"),
                max_azs=1,
                nat_gateways=0,
                enable_dns_support=True,
                enable_dns_hostnames=True,
                subnet_configuration=[
                    _ec2.SubnetConfiguration(
                        name="public", cidr_mask=24, subnet_type=_ec2.SubnetType.PUBLIC
                    ),
                    # _ec2.SubnetConfiguration(
                    #     name="app", cidr_mask=24, subnet_type=_ec2.SubnetType.PRIVATE
                    # ),
                    _ec2.SubnetConfiguration(
                        name="db",
                        cidr_mask=24,
                        subnet_type=_ec2.SubnetType.PRIVATE_ISOLATED,
                    ),
                ],
            )

    # properties to share with other stacks
    @property
    def get_vpc(self):
        return self.vpc

    @property
    def get_vpc_public_subnet_ids(self):
        return self.vpc.select_subnets(subnet_type=_ec2.SubnetType.PUBLIC).subnet_ids

    @property
    def get_vpc_private_subnet_ids(self):
        return self.vpc.select_subnets(subnet_type=_ec2.SubnetType.PRIVATE).subnet_ids
