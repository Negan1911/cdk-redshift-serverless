/* eslint-disable-next-line import/no-unresolved */
import * as AWSLambda from 'aws-lambda';
import { executeStatement } from './redshift-data';
import { NamespaceProps } from './types';
import { makePhysicalId } from './util';
import { UserGenericProps } from '../handler-props';


export async function handler(props: UserGenericProps & NamespaceProps, event: AWSLambda.CloudFormationCustomResourceEvent) {
  const username = props.username;
  const namespaceProps = props;

  if (event.RequestType === 'Create') {
    await createUser(username, namespaceProps);
    return { PhysicalResourceId: makePhysicalId(username, namespaceProps, event.RequestId), Data: { username: username } };
  } else if (event.RequestType === 'Delete') {
    await dropUser(username, namespaceProps);
    return;
  } else if (event.RequestType === 'Update') {
    const { replace } = await updateUser(username, namespaceProps, event.OldResourceProperties as UserGenericProps & NamespaceProps);
    const physicalId = replace ? makePhysicalId(username, namespaceProps, event.RequestId) : event.PhysicalResourceId;
    return { PhysicalResourceId: physicalId, Data: { username: username } };
  } else {
    /* eslint-disable-next-line dot-notation */
    throw new Error(`Unrecognized event type: ${event['RequestType']}`);
  }
}

async function dropUser(username: string, namespaceProps: NamespaceProps) {
  await executeStatement(`DROP USER "${username}"`, namespaceProps);
}

async function createUser(username: string, namespaceProps: NamespaceProps) {
  await executeStatement(`CREATE USER "${username}" PASSWORD DISABLE`, namespaceProps);
}

async function updateUser(
  username: string,
  namespaceProps: NamespaceProps,
  oldResourceProperties: UserGenericProps & NamespaceProps,
): Promise<{ replace: boolean }> {
  const oldNamespaceProps = oldResourceProperties;
  if (namespaceProps.namespaceName !== oldNamespaceProps.namespaceName || namespaceProps.databaseName !== oldNamespaceProps.databaseName) {
    await createUser(username, namespaceProps);
    return { replace: true };
  }

  const oldUsername = oldResourceProperties.username;

  if (username !== oldUsername) {
    await createUser(username, namespaceProps);
    return { replace: true };
  }

  return { replace: false };
}