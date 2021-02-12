/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import each from 'jest-each';
import { cloneDeep } from 'lodash';
import DynamoDbParamBuilder from './dynamoDbParamBuilder';
import DOCUMENT_STATUS from './documentStatus';
import { timeFromEpochInMsRegExp, utcTimeRegExp } from '../../testUtilities/regExpressions';

describe('buildUpdateDocumentStatusParam', () => {
    const resourceType = 'Patient';
    test('Update status correctly when there is a requirement for what the old status needs to be', () => {
        each([
            ['', ''],
            ['custom-tenant', '-custom-tenant'],
        ]).it(
            'Update status correctly when there is a requirement for what the old status needs to be. Tenant is "%s"',
            (tenantId: string, tableName: string) => {
                const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
                const vid = 1;

                // Check that the old status is AVAILABLE before changing it to LOCK
                const actualParam = DynamoDbParamBuilder.buildUpdateDocumentStatusParam(
                    DOCUMENT_STATUS.AVAILABLE,
                    DOCUMENT_STATUS.LOCKED,
                    id,
                    vid,
                    resourceType,
                    tenantId,
                );

                const expectedParam = {
                    Update: {
                        TableName: tableName,
                        Key: {
                            id: {
                                S: id,
                            },
                            vid: {
                                N: vid.toString(),
                            },
                        },
                        UpdateExpression: 'set documentStatus = :newStatus, lockEndTs = :futureEndTs',
                        ExpressionAttributeValues: {
                            ':newStatus': {
                                S: 'LOCKED',
                            },
                            ':oldStatus': {
                                S: 'AVAILABLE',
                            },
                            ':pendingDeleteStatus': {
                                S: 'PENDING_DELETE',
                            },
                            ':pendingStatus': {
                                S: 'PENDING',
                            },
                            ':resourceType': {
                                S: 'Patient',
                            },
                            ':lockStatus': {
                                S: 'LOCKED',
                            },
                            ':currentTs': {
                                N: expect.stringMatching(timeFromEpochInMsRegExp),
                            },
                            ':futureEndTs': {
                                N: expect.stringMatching(timeFromEpochInMsRegExp),
                            },
                        },
                        ConditionExpression:
                            'resourceType = :resourceType AND (documentStatus = :oldStatus OR (lockEndTs < :currentTs AND (documentStatus = :lockStatus OR documentStatus = :pendingStatus OR documentStatus = :pendingDeleteStatus)))',
                    },
                };

                const futureTs = Number(actualParam.Update.ExpressionAttributeValues[':futureEndTs'].N);
                const currentTs = Number(actualParam.Update.ExpressionAttributeValues[':currentTs'].N);

                expect(futureTs).toEqual(currentTs + DynamoDbParamBuilder.LOCK_DURATION_IN_MS);
                expect(actualParam).toEqual(expectedParam);
            },
        );
    });

    const getExpectedParamForUpdateWithoutOldStatus = (
        documentStatus: DOCUMENT_STATUS,
        id: string,
        vid: number,
        tableName: string,
    ) => {
        return {
            Update: {
                TableName: tableName,
                Key: {
                    id: {
                        S: id,
                    },
                    vid: {
                        N: vid.toString(),
                    },
                },
                UpdateExpression: 'set documentStatus = :newStatus, lockEndTs = :futureEndTs',
                ConditionExpression: 'resourceType = :resourceType',
                ExpressionAttributeValues: {
                    ':newStatus': {
                        S: documentStatus,
                    },
                    ':futureEndTs': {
                        N: expect.stringMatching(timeFromEpochInMsRegExp),
                    },
                    ':resourceType': {
                        S: 'Patient',
                    },
                },
            },
        };
    };
    const wiggleRoomInMs = 1 * 300;

    test('When a document is being locked, lockEndTs should have a timestamp that expires in the future', () => {
        each([
            ['', ''],
            ['custom-tenant', '-custom-tenant'],
        ]).it(
            'When a document is being locked, lockEndTs should have a timestamp that expires in the future. Tenant is "%s"',
            (tenantId: string, tableName: string) => {
                const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
                const vid = 1;
                const actualParam = DynamoDbParamBuilder.buildUpdateDocumentStatusParam(
                    null,
                    DOCUMENT_STATUS.LOCKED,
                    id,
                    vid,
                    resourceType,
                    tenantId,
                );

                const futureTs = Number(actualParam.Update.ExpressionAttributeValues[':futureEndTs'].N);
                // We have to generate the current time, because when there is no requirement for an oldStatus, the expected param doesn't
                // have a currentTs value as part of the query
                const currentTs = Date.now();

                // Future timeStamp should be approximately DynamoDbParamBuilder.LOCK_DURATION_IN_MS time from now
                expect(futureTs).toBeLessThanOrEqual(currentTs + DynamoDbParamBuilder.LOCK_DURATION_IN_MS + wiggleRoomInMs);
                expect(futureTs).toBeGreaterThanOrEqual(
                    currentTs + DynamoDbParamBuilder.LOCK_DURATION_IN_MS - wiggleRoomInMs,
                );

                expect(actualParam).toEqual(
                    getExpectedParamForUpdateWithoutOldStatus(DOCUMENT_STATUS.LOCKED, id, vid, tableName),
                );
            },
        );
    });

    test('Update status correctly when there is NO requirement for what the old status needs to be', () => {
        each([
            ['', ''],
            ['custom-tenant', '-custom-tenant'],
        ]).it(
            'Update status correctly when there is NO requirement for what the old status needs to be. Tenant is "%s"',
            (tenantId: string, tableName: string) => {
                const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
                const vid = 1;
                // Check the status to be AVAILABLE no matter what the previous status was
                const actualParam = DynamoDbParamBuilder.buildUpdateDocumentStatusParam(
                    null,
                    DOCUMENT_STATUS.AVAILABLE,
                    id,
                    vid,
                    resourceType,
                    tenantId,
                );

                const futureTs = Number(actualParam.Update.ExpressionAttributeValues[':futureEndTs'].N);
                // We have to generate the current time, because when there is no requirement for an oldStatus, the expected param doesn't
                // have a currentTs value as part of the query
                const currentTs = Date.now();
                // FutureTs should be approximately now
                expect(futureTs).toBeLessThanOrEqual(currentTs + wiggleRoomInMs);
                expect(futureTs).toBeGreaterThanOrEqual(currentTs - wiggleRoomInMs);
                expect(actualParam).toEqual(
                    getExpectedParamForUpdateWithoutOldStatus(DOCUMENT_STATUS.AVAILABLE, id, vid, tableName),
                );
            },
        );
    });
});

describe('buildPutAvailableItemParam', () => {
    test('check that param has the fields documentStatus, lockEndTs, and references  with default tenant', () => {
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = 1;
        const item = {
            resourceType: 'Patient',
            id,
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            gender: 'male',
            meta: {
                lastUpdated: '2020-03-26T15:46:55.848Z',
                versionId: vid.toString(),
            },
        };
        const tenantId = '';
        const actualParams = DynamoDbParamBuilder.buildPutAvailableItemParam(item, id, vid, tenantId);
        const expectedParams: any = {
            TableName: '',
            Item: {
                _references: {
                    L: [],
                },
                resourceType: {
                    S: 'Patient',
                },
                id: {
                    S: id,
                },
                vid: {
                    N: vid.toString(),
                },
                name: {
                    L: [
                        {
                            M: {
                                family: {
                                    S: 'Jameson',
                                },
                                given: {
                                    L: [
                                        {
                                            S: 'Matt',
                                        },
                                    ],
                                },
                            },
                        },
                    ],
                },
                gender: {
                    S: 'male',
                },
                externalId: {
                    S: id,
                },
                meta: {
                    M: {
                        lastUpdated: {
                            S: expect.stringMatching(utcTimeRegExp),
                        },
                        versionId: {
                            S: '1',
                        },
                    },
                },
                documentStatus: {
                    S: 'AVAILABLE',
                },
                lockEndTs: {
                    N: expect.stringMatching(timeFromEpochInMsRegExp),
                },
            },
            ConditionExpression: 'attribute_not_exists(id)',
        };

        expect(actualParams).toEqual(expectedParams);
    });
    test('check that param has the fields documentStatus and lockEndTs with custom tenant', () => {
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = 1;
        const item = {
            resourceType: 'Patient',
            id,
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            gender: 'male',
            meta: {
                lastUpdated: '2020-03-26T15:46:55.848Z',
                versionId: vid.toString(),
            },
        };
        const tenantId = '123';
        const actualParams = DynamoDbParamBuilder.buildPutAvailableItemParam(item, id, vid, tenantId);
        const expectedParams = {
            TableName: '-123',
            Item: {
                _references: {
                    L: [],
                },
                resourceType: {
                    S: 'Patient',
                },
                id: {
                    S: id,
                },
                vid: {
                    N: vid.toString(),
                },
                name: {
                    L: [
                        {
                            M: {
                                family: {
                                    S: 'Jameson',
                                },
                                given: {
                                    L: [
                                        {
                                            S: 'Matt',
                                        },
                                    ],
                                },
                            },
                        },
                    ],
                },
                gender: {
                    S: 'male',
                },
                externalId: {
                    S: id,
                },
                meta: {
                    M: {
                        lastUpdated: {
                            S: '2020-03-26T15:46:55.848Z',
                        },
                        versionId: {
                            S: '1',
                        },
                    },
                },
                documentStatus: {
                    S: 'AVAILABLE',
                },
                lockEndTs: {
                    N: expect.stringMatching(timeFromEpochInMsRegExp),
                },
            },
        };

        expect(actualParams).toEqual(expectedParams);
    });
});

describe('Multi-tenancy in DynamoDB', () => {
    const resourceType = 'Patient';
    test('buildUpdateDocumentStatusParam', () => {
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = 1;
        const tenantId = '123';
        const tableName = '-123';
        const result = DynamoDbParamBuilder.buildUpdateDocumentStatusParam(
            DOCUMENT_STATUS.AVAILABLE,
            DOCUMENT_STATUS.LOCKED,
            id,
            vid,
            resourceType,
            tenantId,
        );
        expect(result.Update.TableName).toEqual(tableName); // RESOURCE_TABLE is ''
    });
    test('buildPutAvailableItemParam', () => {
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = 1;
        const item = {};
        const tenantId = '123';
        const tableName = '-123';
        const result = DynamoDbParamBuilder.buildPutAvailableItemParam(item, id, vid, tenantId);
        expect(result.TableName).toEqual(tableName); // RESOURCE_TABLE is ''
    });
    test('buildGetItemParam', () => {
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = 1;
        const tenantId = '123';
        const tableName = '-123';
        const result = DynamoDbParamBuilder.buildGetItemParam(id, vid, tenantId);
        expect(result.TableName).toEqual(tableName); // RESOURCE_TABLE is ''
    });

    test('buildDeleteParam', () => {
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = 1;
        const tenantId = '123';
        const tableName = '-123';
        const result = DynamoDbParamBuilder.buildDeleteParam(id, vid, tenantId);
        expect(result.Delete.TableName).toEqual(tableName); // RESOURCE_TABLE is ''
    });

    test('buildGetResourcesQueryParam', () => {
        const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
        const vid = 1;
        const tenantId = '123';
        const tableName = '-123';
        const result = DynamoDbParamBuilder.buildGetResourcesQueryParam(id, vid, tenantId);
        expect(result.TableName).toEqual(tableName); // RESOURCE_TABLE is ''
    });
});

describe('buildGetResourcesQueryParam', () => {
    const id = '8cafa46d-08b4-4ee4-b51b-803e20ae8126';
    const expectedParam = {
        TableName: '',
        ScanIndexForward: false,
        Limit: 2,
        FilterExpression: '#r = :resourceType',
        KeyConditionExpression: 'id = :hkey',
        ExpressionAttributeNames: { '#r': 'resourceType' },
        ExpressionAttributeValues: {
            ':hkey': { S: '8cafa46d-08b4-4ee4-b51b-803e20ae8126' },
            ':resourceType': { S: 'Patient' },
        },
    };
    test('Param without projection expression', () => {
        const actualParam = DynamoDbParamBuilder.buildGetResourcesQueryParam(id, 'Patient', 2);
        expect(actualParam).toEqual(expectedParam);
    });

    test('Param with projection expression', () => {
        const projectionExpression = 'id, resourceType, name';
        const actualParam = DynamoDbParamBuilder.buildGetResourcesQueryParam(id, 'Patient', 2, projectionExpression);

        const clonedExpectedParam: any = cloneDeep(expectedParam);
        clonedExpectedParam.ProjectionExpression = projectionExpression;

        expect(actualParam).toEqual(clonedExpectedParam);
    });
});
