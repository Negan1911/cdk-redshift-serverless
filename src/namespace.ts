import * as path from 'path';
import * as cdk from 'aws-cdk-lib'
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as kms from 'aws-cdk-lib/aws-kms';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import { DatabaseSecret } from './database-secret';
import * as redshift from 'aws-cdk-lib/aws-redshiftserverless';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import { ArnFormat, CustomResource, Duration, IResource, Lazy, RemovalPolicy, Resource, SecretValue, Stack, Token } from 'aws-cdk-lib';
import { AwsCustomResource, AwsCustomResourcePolicy, PhysicalResourceId, Provider } from 'aws-cdk-lib/custom-resources';


/**
 * Create a Redshift Namespace with a given number of nodes.
 * Implemented by `Cluster` via `ClusterBase`.
 */
export interface INamespace extends IResource {
  /**
     * The name of the namespace. Must be between 3-64 alphanumeric characters in lowercase, and it cannot be a reserved word. A list of reserved words can be found in [Reserved Words](https://docs.aws.amazon.com//redshift/latest/dg/r_pg_keywords.html) in the Amazon Redshift Database Developer Guide.
     *
     * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-namespacename
     */
  readonly namespaceName: string;

  /**
   * The name of the primary database created in the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-dbname
   */
  readonly dbName: string;
}

/**
 * Properties that describe an existing cluster instance
 */
export interface ClusterAttributes {
  /**
   * The security groups of the redshift cluster
   *
   * @default no security groups will be attached to the import
   */
  readonly securityGroups?: ec2.ISecurityGroup[];

  /**
   * Identifier for the cluster
   */
  readonly clusterName: string;

  /**
   * Cluster endpoint address
   */
  readonly clusterEndpointAddress: string;

  /**
   * Cluster endpoint port
   */
  readonly clusterEndpointPort: number;

}

/**
 * Properties for a new database cluster
 */
export interface NamespaceProps {

  /**
   * The name of the namespace. Must be between 3-64 alphanumeric characters in lowercase, and it cannot be a reserved word. A list of reserved words can be found in [Reserved Words](https://docs.aws.amazon.com//redshift/latest/dg/r_pg_keywords.html) in the Amazon Redshift Database Developer Guide.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-namespacename
   */
  readonly namespaceName: string;

  /**
     * The username of the administrator for the primary database created in the namespace.
     *
     * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-adminusername
     */
  adminUsername?: string;
  /**
   * The password of the administrator for the primary database created in the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-adminuserpassword
   */
  adminUserPassword?: string;

  /**
     * The name of the primary database created in the namespace.
     *
     * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-dbname
     */
  readonly dbName: string;
  /**
   * The Amazon Resource Name (ARN) of the IAM role to set as a default in the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-defaultiamrolearn
   */
  readonly defaultIamRole?: iam.Role;
  /**
   * The name of the snapshot to be created before the namespace is deleted.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-finalsnapshotname
   */
  readonly finalSnapshotName?: string;
  /**
   * How long to retain the final snapshot.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-finalsnapshotretentionperiod
   */
  readonly finalSnapshotRetentionPeriod?: number;
  /**
   * A list of IAM roles to associate with the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-iamroles
   */
  readonly iamRoles?: iam.Role[];
  /**
   * AWS Key Management Service key used to encrypt your data.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-kmskeyid
   */
  readonly kmsKey?: kms.Key;
  /**
   * The types of logs the namespace can export. Available export types are `userlog` , `connectionlog` , and `useractivitylog` .
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-logexports
   */
  readonly logExports?: string[];
  /**
   * The map of the key-value pairs used to tag the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-tags
   */
  readonly tags?: cdk.CfnTag[];

  readonly removalPolicy?: RemovalPolicy;
}

/**
 * A new or imported clustered database.
 */
abstract class NamespaceBase extends Resource implements INamespace {

  /**
   * Name of the namespace
   */
  public abstract readonly namespaceName: string;

  /**
   * The name of the primary database created in the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-dbname
   */
  public abstract readonly dbName: string;
}

/**
 * Create a Redshift cluster a given number of nodes.
 *
 * @resource AWS::Redshift::Cluster
 */
export class Namespace extends NamespaceBase {
  /**
   * The username of the administrator for the first database created in the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-properties-redshiftserverless-namespace-namespace.html#cfn-redshiftserverless-namespace-namespace-adminusername
   */
  readonly adminUsername?: string;

  /**
   * The name of the namespace. Must be between 3-64 alphanumeric characters in lowercase, and it cannot be a reserved word. A list of reserved words can be found in [Reserved Words](https://docs.aws.amazon.com//redshift/latest/dg/r_pg_keywords.html) in the Amazon Redshift Database Developer Guide.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-namespacename
   */
  namespaceName: string;
  
  /**
   * The name of the primary database created in the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-dbname
   */
  dbName: string;
  /**
   * The Amazon Resource Name (ARN) of the IAM role to set as a default in the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-defaultiamrolearn
   */
  defaultIamRole?: iam.Role;
  /**
   * The name of the snapshot to be created before the namespace is deleted.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-finalsnapshotname
   */
  finalSnapshotName: string | undefined;
  /**
   * How long to retain the final snapshot.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-finalsnapshotretentionperiod
   */
  finalSnapshotRetentionPeriod: number | undefined;
  /**
   * A list of IAM roles to associate with the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-iamroles
   */
  iamRoles: iam.Role[];
  /**
   * The ID of the AWS Key Management Service key used to encrypt your data.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-kmskeyid
   */
  kmsKey: kms.Key | undefined;
  /**
   * The types of logs the namespace can export. Available export types are `userlog` , `connectionlog` , and `useractivitylog` .
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-logexports
   */
  logExports: string[] | undefined;
  /**
   * The map of the key-value pairs used to tag the namespace.
   *
   * @link http://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-redshiftserverless-namespace.html#cfn-redshiftserverless-namespace-tags
   */
  readonly tags: cdk.CfnTag[];
  
  readonly adminUser: secretsmanager.ISecret; 

  /**
   * The underlying Namespace
   */
  readonly cfnNamespace: redshift.CfnNamespace;

  static fromNamespaceAttributes(scope: Construct, id: string, nsName: string, dbName: string): INamespace {
    return new class extends NamespaceBase {
      public namespaceName: string = nsName;
      public dbName: string = dbName;
    }(scope, id);
  }


  constructor(scope: Construct, id: string, props: NamespaceProps) {
    super(scope, id);

    this.namespaceName = props.namespaceName
    this.dbName = props.dbName
    this.defaultIamRole = props.defaultIamRole
    this.finalSnapshotName = props.finalSnapshotName
    this.finalSnapshotRetentionPeriod = props.finalSnapshotRetentionPeriod
    this.iamRoles = props?.iamRoles || []
    this.kmsKey = props.kmsKey
    this.logExports = props.logExports
    this.tags = props.tags || []

    const removalPolicy = props.removalPolicy ?? RemovalPolicy.RETAIN;

    this.adminUser = props.adminUserPassword
      ? new secretsmanager.Secret(this, 'Secret', {
        secretObjectValue: {
          username: SecretValue.unsafePlainText(props.adminUsername || 'admin'),
          password: SecretValue.unsafePlainText(props.adminUserPassword)
        },
    }) : new DatabaseSecret(this, 'Secret', {
      username: props.adminUsername || 'admin',
      encryptionKey: props.kmsKey,
    })

    this.cfnNamespace = new redshift.CfnNamespace(this, 'Resource', {
      namespaceName: this.namespaceName,
      adminUsername: this.adminUser?.secretValueFromJson('username').unsafeUnwrap(),
      adminUserPassword: this.adminUser?.secretValueFromJson('password').unsafeUnwrap(),
      dbName: this.dbName,
      defaultIamRoleArn: this.defaultIamRole?.roleArn,
      finalSnapshotName: this.finalSnapshotName,
      finalSnapshotRetentionPeriod: this.finalSnapshotRetentionPeriod,
      iamRoles: Lazy.list({ produce: () => this.iamRoles.map(role => role.roleArn) }, { omitEmpty: true }),
      kmsKeyId: this.kmsKey?.keyId,
      logExports: this.logExports,
      tags: this.tags,
    });

    this.cfnNamespace.applyRemovalPolicy(removalPolicy, {
      applyToUpdateReplacePolicy: true,
    });

    this.namespaceName = this.cfnNamespace.ref;
  }

  /**
   * Adds a role to the cluster
   *
   * @param role the role to add
   */
  public addIamRole(role: iam.Role): void {
    const clusterRoleList = this.iamRoles;

    if (clusterRoleList.includes(role)) {
      throw new Error(`Role '${role.roleArn}' is already attached to the cluster`);
    }

    clusterRoleList.push(role);
  }
}