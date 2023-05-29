# cdk-redshift-serverless

This AWS CDK construct enables the creation of Tables for your Redshift Serverless Database. The construct is written in TypeScript and uses the AWS CDK Custom Resources to create the required AWS resources.

Requirements To use this construct, you'll need the following:

An AWS account with the necessary permissions to execute SQL Statements in your redshift instance and AWS CloudFormation resources.
AWS CDK v2 installed and configured on your development machine.
Node.js and TypeScript installed on your development machine.

## Getting Started

To get started with this library, follow these steps:

1. Install the library in your AWS CDK project by running the following command:

```bash
npm install cdk-redshift-serverless
```

or, with yarn:

```bash
yarn add cdk-redshift-serverless
```

2. Import it in your CDK stack:

```ts
import { Table } from "cdk-redshift-serverless";
```

3. Make sure you have your Namespace and Workgroup Set:

```ts
import * as redshift from "aws-cdk-lib/aws-redshiftserverless";

const redshiftNamespace = new redshift.CfnNamespace(this, "RedshiftNamespace", {
  dbName: "your-db-name",
  namespaceName: "your-namespace-name",
  adminUsername: "admin",
  adminUserPassword: "your-pass",
  defaultIamRoleArn: "your-role-arn",
  iamRoles: ["your-role-arn"],
  kmsKeyId: "your-key-id",
});

const redshiftWorkgroup = new redshift.CfnWorkgroup(this, "RedshiftWorkgroup", {
  workgroupName: "your-work-group-name",
  baseCapacity: 8,
  // configParameters: [],
  publiclyAccessible: true,
  enhancedVpcRouting: false,
  namespaceName: redshiftNamespace.namespaceName,
});
```

4. Create the table by calling:

```ts
const table = new Table(this, "MyTable", {
  tableName: "my-table",
  workGroup: redshiftWorkgroup, // 👈 Reference your workgroup here.
  databaseName: redshiftNamespace.dbName, // 👈 Reference your Db name (from namespace) here.
  tableColumns: [
    { name: "col1", dataType: "varchar(4)", distKey: true },
    { name: "col2", dataType: "float" },
  ],
});
```

5. When you deploy, that table it's going to be generated.
