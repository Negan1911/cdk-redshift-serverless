import * as redshift from 'aws-cdk-lib/aws-redshiftserverless'

/**
 * Properties for accessing a Redshift database
 */
export interface DatabaseOptions {
  /**
   * Workgroup containing the database.
   */
  readonly workGroup: redshift.CfnWorkgroup;

  /**
   * The name of the database.
   */
  readonly databaseName: string;
}