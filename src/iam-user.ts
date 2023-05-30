import * as kms from 'aws-cdk-lib/aws-kms';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as cdk from 'aws-cdk-lib';
import { Construct, IConstruct } from 'constructs';
import { DatabaseOptions } from './database-options';
import { DatabaseSecret } from './database-secret';
import { DatabaseQuery } from './private/database-query';
import * as redshift from 'aws-cdk-lib/aws-redshiftserverless';
import { HandlerName } from './private/database-query-provider/handler-name';
import { UserGenericProps } from './private/handler-props';
import { UserTablePrivileges } from './private/privileges';
import { ITable, TableAction } from './table';

/**
 * Properties for configuring a Redshift user.
 */
export interface IAMUserProps extends DatabaseOptions {
  /**
   * The name of the user.
   *
   * For valid values, see: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
   *
   * @default - a name is generated
   */
  readonly username?: string;

  /**
   * The name of the role.
   *
   * For valid values, see: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
   *
   */
  readonly role?: iam.IRole;

  /**
   * The policy to apply when this resource is removed from the application.
   *
   * @default cdk.RemovalPolicy.Destroy
   */
  readonly removalPolicy?: cdk.RemovalPolicy;
}

/**
 * Represents a user in a Redshift database.
 */
export interface IIAMUser extends IConstruct {
  /**
   * The name of the user.
   */
  readonly username: string;

  /**
   * The Workgroup containing the database.
   */
  readonly workGroup: redshift.CfnWorkgroup;

  /**
   * The Workgroup containing the database.
   */
  readonly namespace: redshift.CfnNamespace;

  /**
   * Grant this user privilege to access a table.
   */
  addTablePrivileges(table: ITable, ...actions: TableAction[]): void;
}

/**
 * A full specification of a Redshift user that can be used to import it fluently into the CDK application.
 */
export interface IAMUserAttributes extends DatabaseOptions {
  /**
   * The name of the user.
   */
  readonly username: string;
}

abstract class UserBase extends Construct implements IIAMUser {
  abstract readonly username: string;
  abstract readonly workGroup: redshift.CfnWorkgroup;
  abstract readonly namespace: redshift.CfnNamespace;

  /**
   * The tables that user will have access to
   */
  private privileges?: UserTablePrivileges;

  protected abstract readonly databaseProps: DatabaseOptions;

  addTablePrivileges(table: ITable, ...actions: TableAction[]): void {
    if (!this.privileges) {
      this.privileges = new UserTablePrivileges(this, 'TablePrivileges', {
        ...this.databaseProps,
        user: this,
      });
    }

    this.privileges.addPrivileges(table, ...actions);
  }
}

/**
 * A user in a Redshift cluster.
 */
export class IAMUser extends UserBase {
  /**
   * Specify a Redshift user using credentials that already exist.
   */
  static fromUserAttributes(scope: Construct, id: string, attrs: IAMUserAttributes): IIAMUser {
    return new class extends UserBase {
      readonly username = attrs.username;
      readonly workGroup = attrs.workGroup;
      readonly namespace = attrs.namespace;
      protected readonly databaseProps = attrs;
    }(scope, id);
  }

  readonly username: string;
  readonly workGroup: redshift.CfnWorkgroup;
  readonly namespace: redshift.CfnNamespace;
  protected databaseProps: DatabaseOptions;

  private resource: DatabaseQuery<UserGenericProps>;

  constructor(scope: Construct, id: string, props: IAMUserProps) {
    super(scope, id);

    this.databaseProps = props;
    this.workGroup = props.workGroup;
    this.namespace = props.namespace;

    if (!props.username && !props.role) {
      throw new Error('Either username or role must be specified');
    }

    const username = props.role ? `IAMR:${props.role.roleName}` : `IAM:${props.username}`;
    
    this.resource = new DatabaseQuery<UserGenericProps>(this, 'Resource', {
      ...this.databaseProps,
      handler: HandlerName.IAMUser,
      properties: { username },
    });
    
    
    this.username = this.resource.getAttString('username');
  }

  /**
   * Apply the given removal policy to this resource
   *
   * The Removal Policy controls what happens to this resource when it stops
   * being managed by CloudFormation, either because you've removed it from the
   * CDK application or because you've made a change that requires the resource
   * to be replaced.
   *
   * The resource can be destroyed (`RemovalPolicy.DESTROY`), or left in your AWS
   * account for data recovery and cleanup later (`RemovalPolicy.RETAIN`).
   *
   * This resource is destroyed by default.
   */
  public applyRemovalPolicy(policy: cdk.RemovalPolicy): void {
    this.resource.applyRemovalPolicy(policy);
  }
}