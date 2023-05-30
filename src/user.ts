import * as kms from 'aws-cdk-lib/aws-kms';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as cdk from 'aws-cdk-lib';
import { Construct, IConstruct } from 'constructs';
import { DatabaseOptions } from './database-options';
import { DatabaseSecret } from './database-secret';
import { DatabaseQuery } from './private/database-query';
import * as redshift from 'aws-cdk-lib/aws-redshiftserverless';
import { HandlerName } from './private/database-query-provider/handler-name';
import { UserHandlerProps } from './private/handler-props';
import { UserTablePrivileges } from './private/privileges';
import { ITable, TableAction } from './table';
import { INamespace } from './namespace';

/**
 * Properties for configuring a Redshift user.
 */
export interface UserProps extends DatabaseOptions {
  /**
   * The name of the user.
   *
   * For valid values, see: https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
   *
   * @default - a name is generated
   */
  readonly username?: string;

  /**
   * KMS key to encrypt the generated secret.
   *
   * @default - the default AWS managed key is used
   */
  readonly encryptionKey?: kms.IKey;

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
export interface IUser extends IConstruct {
  /**
   * The name of the user.
   */
  readonly username: string;

  /**
   * The password of the user.
   */
  readonly password: cdk.SecretValue;

  /**
   * The Workgroup containing the database.
   */
  readonly workGroup: redshift.CfnWorkgroup;

  /**
   * The Workgroup containing the database.
   */
  readonly namespace: INamespace;

  /**
   * Grant this user privilege to access a table.
   */
  addTablePrivileges(table: ITable, ...actions: TableAction[]): void;
}

/**
 * A full specification of a Redshift user that can be used to import it fluently into the CDK application.
 */
export interface UserAttributes extends DatabaseOptions {
  /**
   * The name of the user.
   */
  readonly username: string;

  /**
   * The password of the user.
   *
   * Do not put passwords in CDK code directly.
   */
  readonly password: cdk.SecretValue;
}

abstract class UserBase extends Construct implements IUser {
  abstract readonly username: string;
  abstract readonly password: cdk.SecretValue;
  abstract readonly workGroup: redshift.CfnWorkgroup;
  abstract readonly namespace: INamespace;

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
export class User extends UserBase {
  /**
   * Specify a Redshift user using credentials that already exist.
   */
  static fromUserAttributes(scope: Construct, id: string, attrs: UserAttributes): IUser {
    return new class extends UserBase {
      readonly username = attrs.username;
      readonly password = attrs.password;
      readonly workGroup = attrs.workGroup;
      readonly namespace = attrs.namespace;
      protected readonly databaseProps = attrs;
    }(scope, id);
  }

  readonly username: string;
  readonly password: cdk.SecretValue;
  readonly workGroup: redshift.CfnWorkgroup;
  readonly namespace: INamespace;
  protected databaseProps: DatabaseOptions;

  /**
   * The Secrets Manager secret of the user.
   * @attribute
   */
  public readonly secret: secretsmanager.ISecret;

  private resource: DatabaseQuery<UserHandlerProps>;

  constructor(scope: Construct, id: string, props: UserProps) {
    super(scope, id);

    this.databaseProps = props;
    this.workGroup = props.workGroup;
    this.namespace = props.namespace;

    const username = props.username ?? cdk.Names.uniqueId(this).toLowerCase();
    const secret = new DatabaseSecret(this, 'Secret', {
      username,
      encryptionKey: props.encryptionKey,
    });
    
    this.password = secret.secretValueFromJson('password');

    this.resource = new DatabaseQuery<UserHandlerProps>(this, 'Resource', {
      ...this.databaseProps,
      handler: HandlerName.User,
      properties: {
        username,
        passwordSecretArn: secret.secretArn,
      },
    });
    
    secret.grantRead(this.resource);

    this.username = this.resource.getAttString('username');
    this.secret = secret;
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