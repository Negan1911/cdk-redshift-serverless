/* eslint-disable-next-line import/no-unresolved */
import * as AWSLambda from 'aws-lambda';
import { executeStatement } from './redshift-data';
import { NamespaceProps } from './types';
import { makePhysicalId } from './util';
import { TablePrivilege, UserTablePrivilegesHandlerProps } from '../handler-props';

export async function handler(props: UserTablePrivilegesHandlerProps & NamespaceProps, event: AWSLambda.CloudFormationCustomResourceEvent) {
  const username = props.username;
  const tablePrivileges = props.tablePrivileges;
  const namespaceProps = props;

  if (event.RequestType === 'Create') {
    await grantPrivileges(username, tablePrivileges, namespaceProps);
    return { PhysicalResourceId: makePhysicalId(username, namespaceProps, event.RequestId) };
  } else if (event.RequestType === 'Delete') {
    await revokePrivileges(username, tablePrivileges, namespaceProps);
    return;
  } else if (event.RequestType === 'Update') {
    const { replace } = await updatePrivileges(
      username,
      tablePrivileges,
      namespaceProps,
      event.OldResourceProperties as UserTablePrivilegesHandlerProps & NamespaceProps,
    );
    const physicalId = replace ? makePhysicalId(username, namespaceProps, event.RequestId) : event.PhysicalResourceId;
    return { PhysicalResourceId: physicalId };
  } else {
    /* eslint-disable-next-line dot-notation */
    throw new Error(`Unrecognized event type: ${event['RequestType']}`);
  }
}

async function revokePrivileges(username: string, tablePrivileges: TablePrivilege[], namespaceProps: NamespaceProps) {
  await Promise.all(tablePrivileges.map(({ tableName, actions }) => {
    return executeStatement(`REVOKE ${actions.join(', ')} ON "${tableName}" FROM "${username}"`, namespaceProps);
  }));
}

async function grantPrivileges(username: string, tablePrivileges: TablePrivilege[], namespaceProps: NamespaceProps) {
  await Promise.all(tablePrivileges.map(({ tableName, actions }) => {
    return executeStatement(`GRANT ${actions.join(', ')} ON "${tableName}" TO "${username}"`, namespaceProps);
  }));
}

async function updatePrivileges(
  username: string,
  tablePrivileges: TablePrivilege[],
  namespaceProps: NamespaceProps,
  oldResourceProperties: UserTablePrivilegesHandlerProps & NamespaceProps,
): Promise<{ replace: boolean }> {
  const oldNamespaceProps = oldResourceProperties;
  if (namespaceProps.workGroupName !== oldNamespaceProps.workGroupName || namespaceProps.databaseName !== oldNamespaceProps.databaseName) {
    await grantPrivileges(username, tablePrivileges, namespaceProps);
    return { replace: true };
  }

  const oldUsername = oldResourceProperties.username;
  if (oldUsername !== username) {
    await grantPrivileges(username, tablePrivileges, namespaceProps);
    return { replace: true };
  }

  const oldTablePrivileges = oldResourceProperties.tablePrivileges;
  if (oldTablePrivileges !== tablePrivileges) {
    await revokePrivileges(username, oldTablePrivileges, namespaceProps);
    await grantPrivileges(username, tablePrivileges, namespaceProps);
    return { replace: false };
  }

  return { replace: false };
}