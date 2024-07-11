from aws_cdk import aws_redshift as _redshift
from aws_cdk import aws_ec2 as _ec2
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_secretsmanager as _sm
from aws_cdk import Stack, RemovalPolicy, CfnOutput, Aws
from constructs import Construct
import os


class GlobalArgs:
    """
    Helper to define global statics
    """

    OWNER = ""
    ENVIRONMENT = ""
    REPO_NAME = ""
    SOURCE_INFO = f""
    VERSION = ""



class RedshiftStack(Stack):

    def __init__(
        self,
        scope: Construct,
        id: str,
        vpc,
        ec2_instance_type: str,
        stack_log_level: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # cluster password
        test_cluster_secret = _sm.Secret(
            self,
            "setRedshiftClusterSecret",
            description="Redshift cluster secret",
            secret_name="RedshiftClusterSecret",
            generate_secret_string=_sm.SecretStringGenerator(exclude_punctuation=True),
            removal_policy=RemovalPolicy.DESTROY,
        )
    
        # Redshift IAM Role
        _rs_cluster_role = _iam.Role(
            self,
            "redshiftClusterRole",
            assumed_by=_iam.ServicePrincipal("redshift.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonS3ReadOnlyAccess"
                )

            ],
        )
        # Subnet group for cluster
        test_cluster_subnet_group = _redshift.CfnClusterSubnetGroup(
            self,
            "redshiftClusterSubnetGroup",
            subnet_ids=vpc.get_vpc_public_subnet_ids,
            description="Redshift Cluster subnet group",
        )
        # Create Security Group for QuickSight
        to_redshift_sg = _ec2.SecurityGroup(
            self,
            id="redshiftSecurityGroup",
            vpc=vpc.get_vpc,
            security_group_name=f"redshift_sg_{id}",
            description="Security Group for testing",
        )

        # https://docs.aws.amazon.com/quicksight/latest/user/regions.html
        to_redshift_sg.add_ingress_rule(
            peer=_ec2.Peer.ipv4("79.160.142.155/27"),  # needs to be updated
            connection=_ec2.Port.tcp(5439),
            description="Allow connetions",
        )

        # Create cluster
        test_cluster = _redshift.CfnCluster(
            self,
            "redshiftCluster",
            cluster_type="single-node",
            db_name="test_db",
            master_username="dwh_user",
            master_user_password="6QO52sfZC0aVd8bmUkh$Zjy9HEsPeeGsC9C3d9stO",  # demo_cluster_secret.secret_value.to_string(),
            iam_roles=[_rs_cluster_role.role_arn],
            node_type=ec2_instance_type,
            cluster_subnet_group_name=test_cluster_subnet_group.ref,
            vpc_security_group_ids=[to_redshift_sg.security_group_id],
        )

#        output_0 = CfnOutput(
#            self, "automationForm", value=f"{Globalargs.SOURCE_INFO}", description=""
 #       )
        output_1 = CfnOutput(
            self,
            "RedshiftCluster",
            value=f"{test_cluster.attr_endpoint_address}",
            description=f"RedshiftCluster Endpoint",
        )
        output_2 = CfnOutput(
            self,
            "RedshiftClusterPassword",
            value=(
                f"https://console.aws.amazon.com/secretsmanager/home?region="
                f"eu-north-1"
                f"#/secret?name="
                f"{test_cluster_secret.secret_arn}"
            ),
            description=f"Redshift Cluster Password in Secrets Manager",
        )
        output_3 = CfnOutput(
            self,
            "RedshiftIAMRole",
            value=(f"{_rs_cluster_role.role_arn}"),
            description=f"Redshift Cluster IAM Role Arn",
            ) 
