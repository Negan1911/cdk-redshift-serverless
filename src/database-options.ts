/**
 * Properties for accessing a Redshift database
 */
export interface DatabaseOptions {
  /**
   * Name of the workgroup containing the database.
   */
  readonly workGroupName: string;

  /**
   * The name of the database.
   */
  readonly databaseName: string;
}