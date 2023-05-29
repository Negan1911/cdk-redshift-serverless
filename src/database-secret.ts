import * as kms from 'aws-cdk-lib/aws-kms';
import { Construct } from 'constructs';

/**
 * Construction properties for a DatabaseSecret.
 */
export interface DatabaseSecretProps {
  /**
   * The username.
   */
  readonly username: string;

  /**
   * The KMS key to use to encrypt the secret.
   *
   * @default default master key
   */
  readonly encryptionKey?: kms.IKey;
}
