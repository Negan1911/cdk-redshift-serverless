import * as AWSLambda from 'aws-lambda';
import { executeStatement } from './redshift-data';
import { WorkGroupProps } from './types';
import { makePhysicalId } from './util';
import { TablePrivilege, UserTablePrivilegesHandlerProps } from '../handler-props';

export async function handler(props: UserTablePrivilegesHandlerProps & WorkGroupProps, event: AWSLambda.CloudFormationCustomResourceEvent) {
  const username = props.username;
  const tablePrivileges = props.tablePrivileges;
  const workGroupProps = props;

  if (event.RequestType === 'Create') {
    await grantPrivileges(username, tablePrivileges, workGroupProps);
    return { PhysicalResourceId: makePhysicalId(username, workGroupProps, event.RequestId) };
  } else if (event.RequestType === 'Delete') {
    await revokePrivileges(username, tablePrivileges, workGroupProps);
    return;
  } else if (event.RequestType === 'Update') {
    const { replace } = await updatePrivileges(
      username,
      tablePrivileges,
      workGroupProps,
      event.OldResourceProperties as UserTablePrivilegesHandlerProps & WorkGroupProps,
    );
    const physicalId = replace ? makePhysicalId(username, workGroupProps, event.RequestId) : event.PhysicalResourceId;
    return { PhysicalResourceId: physicalId };
  } else {
    /* eslint-disable-next-line dot-notation */
    throw new Error(`Unrecognized event type: ${event['RequestType']}`);
  }
}

async function revokePrivileges(username: string, tablePrivileges: TablePrivilege[], workGroupProps: WorkGroupProps) {
  await Promise.all(tablePrivileges.map(({ tableName, actions }) => {
    return executeStatement(`REVOKE ${actions.join(', ')} ON ${tableName} FROM ${username}`, workGroupProps);
  }));
}

async function grantPrivileges(username: string, tablePrivileges: TablePrivilege[], workGroupProps: WorkGroupProps) {
  await Promise.all(tablePrivileges.map(({ tableName, actions }) => {
    return executeStatement(`GRANT ${actions.join(', ')} ON ${tableName} TO ${username}`, workGroupProps);
  }));
}

async function updatePrivileges(
  username: string,
  tablePrivileges: TablePrivilege[],
  workGroupProps: WorkGroupProps,
  oldResourceProperties: UserTablePrivilegesHandlerProps & WorkGroupProps,
): Promise<{ replace: boolean }> {
  const oldWorkGroupProps = oldResourceProperties;
  if (workGroupProps.workGroupName !== oldWorkGroupProps.workGroupName || workGroupProps.databaseName !== oldWorkGroupProps.databaseName) {
    await grantPrivileges(username, tablePrivileges, workGroupProps);
    return { replace: true };
  }

  const oldUsername = oldResourceProperties.username;
  if (oldUsername !== username) {
    await grantPrivileges(username, tablePrivileges, workGroupProps);
    return { replace: true };
  }

  const oldTablePrivileges = oldResourceProperties.tablePrivileges;
  if (oldTablePrivileges !== tablePrivileges) {
    await revokePrivileges(username, oldTablePrivileges, workGroupProps);
    await grantPrivileges(username, tablePrivileges, workGroupProps);
    return { replace: false };
  }

  return { replace: false };
}