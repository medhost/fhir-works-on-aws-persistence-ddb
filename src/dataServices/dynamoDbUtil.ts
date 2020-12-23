/*
 *  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *  SPDX-License-Identifier: Apache-2.0
 */

import { clone, generateMeta } from 'fhir-works-on-aws-interface';
import { SEPARATOR } from '../constants';
import DOCUMENT_STATUS from './documentStatus';

export const DOCUMENT_STATUS_FIELD = 'documentStatus';
export const LOCK_END_TS_FIELD = 'lockEndTs';
export const EXTERNAL_ID_FIELD = 'externalId';
export const VID_FIELD = 'vid';

export class DynamoDbUtil {
    static cleanItem(item: any) {
        const cleanedItem = clone(item);

        delete cleanedItem[DOCUMENT_STATUS_FIELD];
        delete cleanedItem[LOCK_END_TS_FIELD];
        delete cleanedItem[VID_FIELD];
        delete cleanedItem[EXTERNAL_ID_FIELD];

        // Return id instead of full id (this is only a concern in results from ES)
        const id = item.id.split(SEPARATOR)[0];
        cleanedItem.id = id;

        return cleanedItem;
    }

    static prepItemForDdbInsert(resource: any, id: string, vid: number, documentStatus: DOCUMENT_STATUS) {
        const item = clone(resource);
        item.id = id;
        item.vid = vid;
        if (vid && !item.meta) {
            item.meta = generateMeta(vid.toString());
        }
        if (vid && item.meta && !item.meta.versionId) {
            const generatedMeta = generateMeta(vid.toString());
            item.meta = { ...item.meta, ...generatedMeta };
        }
        item[DOCUMENT_STATUS_FIELD] = documentStatus;
        item[LOCK_END_TS_FIELD] = Date.now();
        item[EXTERNAL_ID_FIELD] = id;
        return item;
    }
}
