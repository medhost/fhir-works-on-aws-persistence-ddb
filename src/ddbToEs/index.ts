/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import AWS from 'aws-sdk';
import DdbToEsHelper from './ddbToEsHelper';
import PromiseParamAndId from './promiseParamAndId';

const REMOVE = 'REMOVE';

// This is a separate lambda function from the main FHIR API server lambda.
// This lambda picks up changes from DDB by way of DDB stream, and sends those changes to ElasticSearch Service for indexing.
// This allows the FHIR API Server to query ElasticSearch service for search requests

export async function handleDdbToEsEvent(event: any) {
    const ddbToEsHelper = new DdbToEsHelper();
    const ssm = new AWS.SSM();
    const { TENANT_PARAMETER_STORE } = process.env;
    try {
        const promiseParamAndIds: PromiseParamAndId[] = [];
        for (let i = 0; i < event.Records.length; i += 1) {
            const record = event.Records[i];
            let tenantId = '';
            if (TENANT_PARAMETER_STORE) {
                console.log('Tenant Parameter Store: ', TENANT_PARAMETER_STORE);
                // eslint-disable-next-line no-await-in-loop
                const returnValue = await ssm
                    .getParameter({
                        Name: TENANT_PARAMETER_STORE,
                        WithDecryption: true,
                    })
                    .promise();

                if (returnValue && returnValue.Parameter && returnValue.Parameter.Value) {
                    console.log('Source ARN: ', record.eventSourceARN);
                    const tenants = JSON.parse(returnValue.Parameter.Value);
                    const arnSplit = record.eventSourceARN.split('/');
                    const arnValue = arnSplit[0].concat('/').concat(arnSplit[1]);
                    console.log('ARN: ', arnValue);

                    // eslint-disable-next-line no-prototype-builtins
                    if (tenants && tenants.hasOwnProperty(arnValue)) {
                        tenantId = tenants[arnValue].trim();
                        console.log('Tenant Id: ', tenantId);
                    }
                }
            }
            const ddbJsonImage = record.eventName === REMOVE ? record.dynamodb.OldImage : record.dynamodb.NewImage;
            const image = AWS.DynamoDB.Converter.unmarshall(ddbJsonImage);
            // Don't index binary files
            if (ddbToEsHelper.isBinaryResource(image)) {
                console.log('This is a Binary resource. These are not searchable');
                // eslint-disable-next-line no-continue
                continue;
            }

            const { resourceType } = image;
            console.log('resourceType: ', resourceType);
            // eslint-disable-next-line no-await-in-loop
            await ddbToEsHelper.createIndexIfNotExist(resourceType.toLowerCase());
            if (record.eventName === REMOVE) {
                // If a user manually deletes a record from DDB, let's delete it from ES also
                const idAndDeletePromise = ddbToEsHelper.getDeleteRecordPromiseParam(
                    image,
                    resourceType.toLowerCase(),
                    tenantId,
                );
                promiseParamAndIds.push(idAndDeletePromise);
            } else {
                const idAndUpsertPromise = ddbToEsHelper.getUpsertRecordPromiseParam(
                    image,
                    resourceType.toLowerCase(),
                    tenantId,
                );
                if (idAndUpsertPromise) {
                    promiseParamAndIds.push(idAndUpsertPromise);
                }
            }
        }

        await ddbToEsHelper.logAndExecutePromises(promiseParamAndIds);
    } catch (e) {
        console.log('Failed to update ES records', e);
    }
}
