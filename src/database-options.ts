import { Workgroup } from './workgroup'
import { INamespace } from './namespace';
import * as redshift from 'aws-cdk-lib/aws-redshiftserverless';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';

/**
 * Properties for accessing a Redshift database
 */
export interface DatabaseOptions {
  /**
   * The Workgroup containing the database.
   */
  readonly workGroup: Workgroup;

  /**
   * The Workgroup containing the database.
   */
  readonly namespace: INamespace;

  /**
   * The secret containing credentials to a Redshift user with administrator privileges.
   *
   * Secret JSON schema: `{ username: string; password: string }`.
   *
   * @default - the admin secret is taken from the cluster
   */
  readonly adminUser: secretsmanager.ISecret;
}