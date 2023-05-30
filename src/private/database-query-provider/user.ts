/* eslint-disable-next-line import/no-unresolved */
import * as AWSLambda from 'aws-lambda';
/* eslint-disable-next-line import/no-extraneous-dependencies */
import SecretsManager from 'aws-sdk/clients/secretsmanager';
import { executeStatement } from './redshift-data';
import { NamespaceProps } from './types';
import { makePhysicalId } from './util';
import { UserHandlerProps } from '../handler-props';

const secretsManager = new SecretsManager();

export async function handler(props: UserHandlerProps & NamespaceProps, event: AWSLambda.CloudFormationCustomResourceEvent) {
  const username = props.username;
  const passwordSecretArn = props.passwordSecretArn;
  const namespaceProps = props;

  if (event.RequestType === 'Create') {
    await createUser(username, passwordSecretArn, namespaceProps);
    return { PhysicalResourceId: makePhysicalId(username, namespaceProps, event.RequestId), Data: { username: username } };
  } else if (event.RequestType === 'Delete') {
    await dropUser(username, namespaceProps);
    return;
  } else if (event.RequestType === 'Update') {
    const { replace } = await updateUser(username, passwordSecretArn, namespaceProps, event.OldResourceProperties as UserHandlerProps & NamespaceProps);
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

async function createUser(username: string, passwordSecretArn: string, namespaceProps: NamespaceProps) {
  const password = await getPasswordFromSecret(passwordSecretArn);

  await executeStatement(`CREATE USER "${username}" PASSWORD '${password}'`, namespaceProps);
}

async function updateUser(
  username: string,
  passwordSecretArn: string,
  namespaceProps: NamespaceProps,
  oldResourceProperties: UserHandlerProps & NamespaceProps,
): Promise<{ replace: boolean }> {
  const oldNamespaceProps = oldResourceProperties;
  if (namespaceProps.namespaceName !== oldNamespaceProps.namespaceName || namespaceProps.databaseName !== oldNamespaceProps.databaseName) {
    await createUser(username, passwordSecretArn, namespaceProps);
    return { replace: true };
  }

  const oldUsername = oldResourceProperties.username;
  const oldPasswordSecretArn = oldResourceProperties.passwordSecretArn;
  const oldPassword = await getPasswordFromSecret(oldPasswordSecretArn);
  const password = await getPasswordFromSecret(passwordSecretArn);

  if (username !== oldUsername) {
    await createUser(username, passwordSecretArn, namespaceProps);
    return { replace: true };
  }

  if (password !== oldPassword) {
    await executeStatement(`ALTER USER "${username}" PASSWORD '${password}'`, namespaceProps);
    return { replace: false };
  }

  return { replace: false };
}

async function getPasswordFromSecret(passwordSecretArn: string): Promise<string> {
  const secretValue = await secretsManager.getSecretValue({
    SecretId: passwordSecretArn,
  }).promise();
  const secretString = secretValue.SecretString;
  if (!secretString) {
    throw new Error(`Secret string for ${passwordSecretArn} was empty`);
  }
  const { password } = JSON.parse(secretString);

  return password;
}