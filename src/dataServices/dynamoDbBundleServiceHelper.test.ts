/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import { BatchReadWriteRequest, BatchReadWriteResponse } from 'fhir-works-on-aws-interface';
import each from 'jest-each';
import DynamoDbBundleServiceHelper from './dynamoDbBundleServiceHelper';
import { DynamoDBConverter } from './dynamoDb';
import GenerateStagingRequestsFactory from '../../testUtilities/GenerateStagingRequestsFactory';
import GenerateRollbackRequestsFactory from '../../testUtilities/GenerateRollbackRequestsFactory';

describe('generateStagingRequests', () => {
    each([
        ['', ''],
        ['custom-tenant', '-custom-tenant'],
    ]).test('CREATE for tenant "%s"', (tenantId, expectedTableName) => {
        const actualResult = DynamoDbBundleServiceHelper.generateStagingRequests(
            [GenerateStagingRequestsFactory.getCreate(expectedTableName).request],
            GenerateStagingRequestsFactory.getCreate(expectedTableName).idToVersionId,
            tenantId,
        );
        const { expectedRequest, expectedLock, expectedStagingResponse } = GenerateStagingRequestsFactory.getCreate(
            expectedTableName,
        );
        const expectedResult: any = {
            deleteRequests: [],
            createRequests: [expectedRequest],
            updateRequests: [],
            readRequests: [],
            newLocks: [expectedLock],
            newStagingResponses: [expectedStagingResponse],
        };
        expect(expectedRequest.Put.TableName.includes(tenantId)).toBeTruthy();
        expect(actualResult).toMatchObject(expectedResult);
    });
    each([
        ['', ''],
        ['custom-tenant', '-custom-tenant'],
    ]).test('READ for tenant "%s"', (tenantId, expectedTableName) => {
        const actualResult = DynamoDbBundleServiceHelper.generateStagingRequests(
            [GenerateStagingRequestsFactory.getRead(expectedTableName).request],
            GenerateStagingRequestsFactory.getRead(expectedTableName).idToVersionId,
            tenantId,
        );
        const { expectedRequest, expectedStagingResponse } = GenerateStagingRequestsFactory.getRead(expectedTableName);
        const expectedResult: any = {
            deleteRequests: [],
            createRequests: [],
            updateRequests: [],
            readRequests: [expectedRequest],
            newLocks: [],
            newStagingResponses: [expectedStagingResponse],
        };
        expect(expectedRequest.Get.TableName.includes(tenantId)).toBeTruthy();
        expect(actualResult).toMatchObject(expectedResult);
    });
    each([
        ['', ''],
        ['custom-tenant', '-custom-tenant'],
    ]).test('UPDATE for tenant "%s"', (tenantId, expectedTableName) => {
        const actualResult = DynamoDbBundleServiceHelper.generateStagingRequests(
            [GenerateStagingRequestsFactory.getUpdate(expectedTableName).request],
            GenerateStagingRequestsFactory.getUpdate(expectedTableName).idToVersionId,
            tenantId,
        );
        const { expectedRequest, expectedLock, expectedStagingResponse } = GenerateStagingRequestsFactory.getUpdate(
            expectedTableName,
        );
        const expectedResult: any = {
            deleteRequests: [],
            createRequests: [],
            updateRequests: [expectedRequest],
            readRequests: [],
            newLocks: [expectedLock],
            newStagingResponses: [expectedStagingResponse],
        };
        expect(expectedRequest.Put.TableName.includes(tenantId)).toBeTruthy();
        expect(actualResult).toMatchObject(expectedResult);
    });
    each(['', 'custom-tenant']).test('DELETE for tenant "%s"', tenantId => {
        const actualResult = DynamoDbBundleServiceHelper.generateStagingRequests(
            [GenerateStagingRequestsFactory.getDelete(tenantId).request],
            GenerateStagingRequestsFactory.getDelete(tenantId).idToVersionId,
            tenantId,
        );
        const { expectedRequest, expectedStagingResponse } = GenerateStagingRequestsFactory.getDelete(tenantId);
        const expectedResult: any = {
            deleteRequests: [expectedRequest],
            createRequests: [],
            updateRequests: [],
            readRequests: [],
            newLocks: [],
            newStagingResponses: [expectedStagingResponse],
        };

        expect(actualResult).toMatchObject(expectedResult);
    });

    each([
        ['', ''],
        ['custom-tenant', '-custom-tenant'],
    ]).test('CRUD for tenant "%s"', (tenantId, expectedTableName) => {
        let idToVersionId: Record<string, number> = {};
        idToVersionId = {
            ...GenerateStagingRequestsFactory.getRead(expectedTableName).idToVersionId,
            ...GenerateStagingRequestsFactory.getUpdate(expectedTableName).idToVersionId,
            ...GenerateStagingRequestsFactory.getDelete(expectedTableName).idToVersionId,
        };

        const requests: BatchReadWriteRequest[] = [
            GenerateStagingRequestsFactory.getCreate(expectedTableName).request,
            GenerateStagingRequestsFactory.getRead(expectedTableName).request,
            GenerateStagingRequestsFactory.getUpdate(expectedTableName).request,
            GenerateStagingRequestsFactory.getDelete(expectedTableName).request,
        ];
        const actualResult = DynamoDbBundleServiceHelper.generateStagingRequests(requests, idToVersionId, tenantId);

        const expectedResult = {
            createRequests: [GenerateStagingRequestsFactory.getCreate(expectedTableName).expectedRequest],
            readRequests: [GenerateStagingRequestsFactory.getRead(expectedTableName).expectedRequest],
            updateRequests: [GenerateStagingRequestsFactory.getUpdate(expectedTableName).expectedRequest],
            deleteRequests: [GenerateStagingRequestsFactory.getDelete(tenantId).expectedRequest],
            newLocks: [
                GenerateStagingRequestsFactory.getCreate(expectedTableName).expectedLock,
                GenerateStagingRequestsFactory.getUpdate(expectedTableName).expectedLock,
            ],
            newStagingResponses: [
                GenerateStagingRequestsFactory.getCreate(expectedTableName).expectedStagingResponse,
                GenerateStagingRequestsFactory.getRead(expectedTableName).expectedStagingResponse,
                GenerateStagingRequestsFactory.getUpdate(expectedTableName).expectedStagingResponse,
                GenerateStagingRequestsFactory.getDelete(expectedTableName).expectedStagingResponse,
            ],
        };

        expect(actualResult).toMatchObject(expectedResult);
    });
});

describe('generateRollbackRequests', () => {
    const testRunner = (operation: any, vid: string) => {
        const bundleEntryResponse = GenerateRollbackRequestsFactory.buildBundleEntryResponse(operation, vid);

        const actualResult = DynamoDbBundleServiceHelper.generateRollbackRequests([bundleEntryResponse]);

        const expectedResult = GenerateRollbackRequestsFactory.buildExpectedBundleEntryResult(bundleEntryResponse);
        expect(actualResult).toEqual(expectedResult);
    };

    test('CREATE', () => {
        testRunner('create', '1');
    });

    test('READ', () => {
        testRunner('read', '1');
    });

    test('UPDATE', () => {
        testRunner('update', '2');
    });

    test('DELETE', () => {
        testRunner('delete', '1');
    });

    test('CRUD', () => {
        const createBundleEntryResponse = GenerateRollbackRequestsFactory.buildBundleEntryResponse('create', '1');
        const readBundleEntryResponse = GenerateRollbackRequestsFactory.buildBundleEntryResponse('read', '1');
        const updateBundleEntryResponse = GenerateRollbackRequestsFactory.buildBundleEntryResponse('update', '1');
        const deleteBundleEntryResponse = GenerateRollbackRequestsFactory.buildBundleEntryResponse('delete', '1');

        const actualResult = DynamoDbBundleServiceHelper.generateRollbackRequests([
            createBundleEntryResponse,
            readBundleEntryResponse,
            updateBundleEntryResponse,
            deleteBundleEntryResponse,
        ]);

        const expectedCreateResult = GenerateRollbackRequestsFactory.buildExpectedBundleEntryResult(
            createBundleEntryResponse,
        );
        const expectedReadResult = GenerateRollbackRequestsFactory.buildExpectedBundleEntryResult(
            readBundleEntryResponse,
        );
        const expectedUpdateResult = GenerateRollbackRequestsFactory.buildExpectedBundleEntryResult(
            updateBundleEntryResponse,
        );
        const expectedDeleteResult = GenerateRollbackRequestsFactory.buildExpectedBundleEntryResult(
            deleteBundleEntryResponse,
        );

        let itemsToRemoveFromLock: any = [];
        itemsToRemoveFromLock = itemsToRemoveFromLock.concat(expectedCreateResult.itemsToRemoveFromLock);
        itemsToRemoveFromLock = itemsToRemoveFromLock.concat(expectedReadResult.itemsToRemoveFromLock);
        itemsToRemoveFromLock = itemsToRemoveFromLock.concat(expectedUpdateResult.itemsToRemoveFromLock);
        itemsToRemoveFromLock = itemsToRemoveFromLock.concat(expectedDeleteResult.itemsToRemoveFromLock);

        itemsToRemoveFromLock = itemsToRemoveFromLock.filter((item: any) => item !== []);

        let transactionRequests: any = [];
        transactionRequests = transactionRequests.concat(expectedCreateResult.transactionRequests);
        transactionRequests = transactionRequests.concat(expectedReadResult.transactionRequests);
        transactionRequests = transactionRequests.concat(expectedUpdateResult.transactionRequests);
        transactionRequests = transactionRequests.concat(expectedDeleteResult.transactionRequests);

        transactionRequests = transactionRequests.filter((req: any) => req !== []);

        expect(actualResult).toEqual({ itemsToRemoveFromLock, transactionRequests });
    });
});

describe('populateBundleEntryResponseWithReadResult', () => {
    test('readResults are merged correctly into bundleEntryResponses', () => {
        const stagingResponses: BatchReadWriteResponse[] = [
            {
                id: '8cafa46d-08b4-4ee4-b51b-803e20ae8126',
                vid: '3',
                operation: 'update',
                lastModified: '2020-04-23T16:22:16.355Z',
                resourceType: 'Patient',
                resource: {},
            },
            {
                id: '3f0830ce-e759-4b07-b75d-577630f2ae4d',
                vid: '1',
                operation: 'create',
                lastModified: '2020-04-23T16:22:16.357Z',
                resourceType: 'Patient',
                resource: {},
            },
            {
                id: '47135b80-b721-430b-9d4b-1557edc64947',
                vid: '1',
                operation: 'read',
                lastModified: '',
                resource: {},
                resourceType: 'Patient',
            },
            {
                id: 'bce8411e-c15e-448c-95dd-69155a837405',
                vid: '1',
                operation: 'delete',
                lastModified: '2020-04-23T16:22:16.357Z',
                resource: {},
                resourceType: 'Patient',
            },
            {
                id: 'vdo49rks-cie9-dkd3-coe0-djei03d83i30',
                vid: '1',
                operation: 'read',
                lastModified: '',
                resource: {},
                resourceType: 'Patient',
            },
        ];

        const firstReadItem = {
            resourceType: 'Patient',
            id: '47135b80-b721-430b-9d4b-1557edc64947',
            vid: '1',
            name: [
                {
                    family: 'Jameson',
                    given: ['Matt'],
                },
            ],
            gender: 'male',
            documentStatus: 'LOCKED',
        };

        const secondReadItem = {
            resourceType: 'Patient',
            id: 'vdo49rks-cie9-dkd3-coe0-djei03d83i30',
            vid: '1',
            name: [
                {
                    family: 'Smith',
                    given: ['Emily'],
                },
            ],
            gender: 'female',
            documentStatus: 'LOCKED',
        };

        const readResult = {
            Responses: [
                {
                    Item: DynamoDBConverter.marshall(firstReadItem),
                },
                {
                    Item: DynamoDBConverter.marshall(secondReadItem),
                },
            ],
        };

        const actualResult = DynamoDbBundleServiceHelper.populateBundleEntryResponseWithReadResult(
            stagingResponses,
            readResult,
        );

        const firstReadStagingResponse = stagingResponses[2];
        firstReadStagingResponse.resource = firstReadItem;

        const secondReadStagingResponse = stagingResponses[4];
        secondReadStagingResponse.resource = secondReadItem;

        const expectedResult = [
            stagingResponses[0],
            stagingResponses[1],
            firstReadStagingResponse,
            stagingResponses[3],
            secondReadStagingResponse,
        ];

        expect(actualResult).toEqual(expectedResult);
    });
});
