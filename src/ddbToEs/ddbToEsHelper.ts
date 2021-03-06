/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import { Client } from '@elastic/elasticsearch';
// @ts-ignore
import { AmazonConnection, AmazonTransport } from 'aws-elasticsearch-connector';
import allSettled from 'promise.allsettled';
import AWS from '../AWS';
import PromiseParamAndId, { PromiseType } from './promiseParamAndId';
import { DOCUMENT_STATUS_FIELD } from '../dataServices/dynamoDbUtil';
import DOCUMENT_STATUS from '../dataServices/documentStatus';

const BINARY_RESOURCE = 'binary';

const { IS_OFFLINE, ELASTICSEARCH_DOMAIN_ENDPOINT } = process.env;

export default class DdbToEsHelper {
    private ElasticSearch: Client;

    constructor() {
        let ES_DOMAIN_ENDPOINT = ELASTICSEARCH_DOMAIN_ENDPOINT || 'https://fake-es-endpoint.com';
        if (IS_OFFLINE === 'true') {
            const { ACCESS_KEY, SECRET_KEY, AWS_REGION, OFFLINE_ELASTICSEARCH_DOMAIN_ENDPOINT } = process.env;

            AWS.config.update({
                region: AWS_REGION || 'us-west-2',
                accessKeyId: ACCESS_KEY,
                secretAccessKey: SECRET_KEY,
            });
            ES_DOMAIN_ENDPOINT = OFFLINE_ELASTICSEARCH_DOMAIN_ENDPOINT || 'https://fake-es-endpoint.com';
        }

        this.ElasticSearch = new Client({
            node: ES_DOMAIN_ENDPOINT,
            Connection: AmazonConnection,
            Transport: AmazonTransport,
        });
    }

    async createIndexIfNotExist(indexName: string) {
        try {
            const indexExistResponse = await this.ElasticSearch.indices.exists({ index: indexName });
            if (!indexExistResponse.body) {
                // Create Index
                const params = {
                    index: indexName,
                    body: {
                        mappings: {
                            properties: {
                                id: {
                                    type: 'keyword',
                                    index: true,
                                },
                                resourceType: {
                                    type: 'keyword',
                                    index: true,
                                },
                                _references: {
                                    type: 'keyword',
                                    index: true,
                                },
                                documentStatus: {
                                    type: 'keyword',
                                    index: true,
                                },
                            },
                        },
                    },
                };
                await this.ElasticSearch.indices.create(params);
            }
        } catch (error) {
            console.log('Failed to check if index exist or create index', error);
        }
    }

    // eslint-disable-next-line class-methods-use-this
    private generateFullId(id: string, vid: number) {
        return `${id}_${vid}`;
    }

    // Getting promise params for actual deletion of the record from ES
    // eslint-disable-next-line class-methods-use-this
    getDeleteRecordPromiseParam(image: any): PromiseParamAndId {
        const lowercaseResourceType = image.resourceType.toLowerCase();

        const { id, vid } = image;
        const compositeId = this.generateFullId(id, vid);
        return {
            promiseParam: {
                index: lowercaseResourceType,
                id: compositeId,
            },
            id: compositeId,
            type: 'delete',
        };
    }

    // Getting promise params for inserting a new record or editing a record
    // eslint-disable-next-line class-methods-use-this
    getUpsertRecordPromiseParam(newImage: any): PromiseParamAndId | null {
        const lowercaseResourceType = newImage.resourceType.toLowerCase();

        // We only perform operations on records with documentStatus === AVAILABLE || DELETED
        if (
            newImage[DOCUMENT_STATUS_FIELD] !== DOCUMENT_STATUS.AVAILABLE &&
            newImage[DOCUMENT_STATUS_FIELD] !== DOCUMENT_STATUS.DELETED
        ) {
            return null;
        }

        let type: PromiseType = 'upsert-DELETED';
        if (newImage[DOCUMENT_STATUS_FIELD] === DOCUMENT_STATUS.AVAILABLE) {
            type = 'upsert-AVAILABLE';
        }
        const { id, vid } = newImage;
        const compositeId = this.generateFullId(id, vid);
        return {
            id: compositeId,
            promiseParam: {
                index: lowercaseResourceType,
                id: compositeId,
                body: {
                    doc: newImage,
                    doc_as_upsert: true,
                },
            },
            type,
        };
    }

    // eslint-disable-next-line class-methods-use-this
    isBinaryResource(image: any): boolean {
        const resourceType = image.resourceType.toLowerCase();
        // Don't index binary files
        return resourceType === BINARY_RESOURCE;
    }

    // eslint-disable-next-line class-methods-use-this
    async logAndExecutePromises(promiseParamAndIds: PromiseParamAndId[]) {
        const upsertAvailablePromiseParamAndIds = promiseParamAndIds.filter(paramAndId => {
            return paramAndId.type === 'upsert-AVAILABLE';
        });

        const upsertDeletedPromiseParamAndIds = promiseParamAndIds.filter(paramAndId => {
            return paramAndId.type === 'upsert-DELETED';
        });

        const deletePromiseParamAndIds = promiseParamAndIds.filter(paramAndId => {
            return paramAndId.type === 'delete';
        });

        console.log(
            `Operation: upsert-AVAILABLE on resource Ids `,
            upsertAvailablePromiseParamAndIds.map(paramAndId => {
                return paramAndId.id;
            }),
        );

        // We're using allSettled-shim because as of 7/21/2020 'serverless-plugin-typescript' does not support
        // Promise.allSettled.
        allSettled.shim();

        // We need to execute creation of a resource before execute deleting of a resource,
        // because a resource can be created and deleted, but not deleted then restored to AVAILABLE
        // @ts-ignore
        await Promise.allSettled(
            upsertAvailablePromiseParamAndIds.map(paramAndId => {
                return this.ElasticSearch.update(paramAndId.promiseParam);
            }),
        );

        console.log(
            `Operation: upsert-DELETED on resource Ids `,
            upsertDeletedPromiseParamAndIds.map(paramAndId => {
                return paramAndId.id;
            }),
        );

        // @ts-ignore
        await Promise.allSettled(
            upsertDeletedPromiseParamAndIds.map(paramAndId => {
                return this.ElasticSearch.update(paramAndId.promiseParam);
            }),
        );

        console.log(
            `Operation: delete on resource Ids `,
            deletePromiseParamAndIds.map(paramAndId => {
                return paramAndId.id;
            }),
        );

        // @ts-ignore
        await Promise.allSettled(
            deletePromiseParamAndIds.map(paramAndId => {
                return this.ElasticSearch.delete(paramAndId.promiseParam);
            }),
        );
    }
}
