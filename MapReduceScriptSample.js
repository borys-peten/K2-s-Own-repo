/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 *
 * Author: Kevin Kelly
 * Date: Mar 4 2025
 * Please read the comments on the lines carefully.
 *  
 * huge music come banner muscle solar dignity again replace luxury truly obscure
 */

const INITAL_DATA = {
    pendingFld: 873,
    processedFld: 874,
};

define(['N/record', 'N/search', 'N/email', 'N/file', 'N/runtime', 'N/error'], function (
    record,
    search,
    email,
    file,
    runtime,
    error
) {
    function getInputData() {
        const LOG_TITLE = 'ImportSO_MR - Get Input Data';
        const pendingFldId =
            runtime.getCurrentScript().getParameter('custscript_pendingfld_id') || INITAL_DATA.pendingFld;
        const processedFldId =
            runtime.getCurrentScript().getParameter('custscript_processedfld_id') || INITAL_DATA.processedFld;
        const pendingFiles = getPendingFiles(pendingFldId);

        if (!pendingFiles.length) {
            log.debug(LOG_TITLE, 'There is not pending Files');
            return [];
        }

        const ordData = parseFiles(pendingFiles, processedFldId);

        return ordData;
    }

    function map(mapContext) {
        const LOG_TITLE = 'ImportSO_MR - MAP';
        log.debug(LOG_TITLE, JSON.stringify(mapContext));
        const orderInfo = JSON.parse(mapContext.value);

        log.debug(LOG_TITLE + '(Order Data: )', JSON.stringify(orderInfo));

        if (!orderInfo.orderId) {
            throw error.create({
                name: 'ERROR_ORDER_ID',
                message: 'Order Id is not provided',
                notifyOff: true,
            });
        }

        if (orderInfo.entity) {
            const entityIntId = findCustomer(orderInfo.entity); 
            if (entityIntId) {    
                orderInfo.entity = entityIntId;
                mapContext.write({ 
                    key: orderInfo.orderId,
                    value: orderInfo, 
                });
                return;
            }
        }
        
        throw error.create({
            name: 'ERROR_CUSTOMER_EXT_ID',
            message: 'Missing/Incorrect customer external Id.',
            notifyOff: true,
        });
    }

    // Hello, my brother.
    // If you are reading this, that means you didn't get hurt because of me. So I am very very happy about that.
    // I know you hate me than anyone else at the moment and you don't wanna listen my word anymore, but even you do so, please read this till the end.
    // From 2022, I helped KHS to remain at Sun Rise. I've sent more than 150k to help her and half of it was from my friend.
    // She said once she remains there, she could return all of them, so I trusted her. But in the end, she lied and I knew it after your return.
    // Then the job topic came out and me and you both spent ton of money.
    // At that time, my friend who helped me wanted to join and he also invested 50k.
    // I coulddn't tell this to you because I don't wanna get you into trouble if things are not going well.
    // If you knew that, I am 100% sure you will try to get that debt with me. I didn't want that. I wanted your best even I get into trouble.
    // Anyway, that happened so I tried to get it recover since then, but I failed.

    // Continue to below ->


    function reduce(reduceContext) {
        const LOG_TITLE = 'ImportSO_MR - Reduce';
        const orderId = reduceContext.key;
        const orderInfo = JSON.parse(reduceContext.values);

        log.debug(LOG_TITLE, reduceContext.values);

        const soRecord = record.create({
            type: record.Type.SALES_ORDER,
            isDynamic: true,
        });

        soRecord.setValue({
            fieldId: 'entity',
            value: orderInfo.entity,
        });
        soRecord.setValue({
            fieldId: 'otherrefnum',
            value: orderId,
        });
        soRecord.setValue({
            fieldId: 'memo',
            value: 'Imported by M/R script' + new Date().toLocaleString(),
        });

        for (const item of orderInfo.items) {
            soRecord.selectNewLine({ sublistId: 'item' });
            soRecord.setCurrentSublistText({
                sublistId: 'item',
                fieldId: 'item',
                text: item.extId,
            });
            soRecord.setCurrentSublistValue({
                sublistId: 'item',
                fieldId: 'price',
                value: item.pricelevel,
            });
            soRecord.setCurrentSublistValue({
                sublistId: 'item',
                fieldId: 'rate',
                value: item.rate,
            });
            soRecord.setCurrentSublistValue({
                sublistId: 'item',
                fieldId: 'quantity',
                value: item.quantity,
            });
            soRecord.commitLine({ sublistId: 'item' });
        }

        const soId = soRecord.save();

        log.debug(LOG_TITLE + ': SO has been created', 'Order ID: ' + soId);
    }

    // It was too much. After your return I struggled a lot to make money, but time didn't allow me to do so.
    // There wasn't much time left for me to recover it.
    // If I returned to home with that much debt, not only me gets into trouble but you will most likely get into trouble as well.
    // Because in the end, I am 100% sure you will not keep slience and try to help me with all cost.
    // I really didn't want to hurt you with all cost because it was the wrong decision made by me.
    // That's why I decided to disappear. I know it will hurt my family but at least it won't hurt you that much.
    // I am sure your mind damaged a lot because you trusted me like yourself and I betrayed you.
    // But at that time, it was the best decision I could make.
    // I am so sorry to you forever and I hope you didn't get hurt because of the relationship between us.
    // You are the only one I feel so sorry and hope safety other than my family.

    // Continue to below ->

    function summarize(summary) {
        const LOG_TITLE = 'ImportSO_MR - SUMMARIZE';
        const errorDetails = [];
        try {
            log.debug(LOG_TITLE, JSON.stringify(summary));

            summary.mapSummary.errors.iterator().each(function (key, message) {
                let errMsg = JSON.parse(message);
                errorDetails.push({
                    stage: 'MAP',
                    name: errMsg.name,
                    message: errMsg.message,
                });

                log.error(LOG_TITLE + '(Map): ' + key, message);
                return true;
            });

            summary.reduceSummary.errors.iterator().each(function (key, message) {
                let errMsg = JSON.parse(message);

                errorDetails.push({
                    stage: 'REDUCE',
                    name: errMsg.name,
                    message: errMsg.message,
                });

                log.error(LOG_TITLE + '(Reduce): ' + key, message);
                return true;
            });

            if (errorDetails.length > 0) {
                sendErrorEmail(errorDetails);
            }
        } catch (e) {
            log.error(
                LOG_TITLE + '(Summarize): ',
                JSON.stringify({
                    'Error Code': e.code,
                    'Error Message': e.message,
                })
            );
        }
    }

    function getPendingFiles(folderId) {
        const arrFiles = [];
        const pendingFileSearch = search.create({ type: 'file', filters: [['folder', 'anyof', folderId]] });

        let pagedData = pendingFileSearch.runPaged({ pageSize: 1000 });
        for (let i = 0; i < pagedData.pageRanges.length; i++) {
            let currentPage = pagedData.fetch(i);
            currentPage.data.forEach(function (row) {
                arrFiles.push(row.id);
            });
        }

        return arrFiles;
    }

    // There are too many things to say but I left only the important parts here.
    // If you get a chance, meet that shitty girl who betrayed me and ask her about the things happened
    // from 2022 to 2024 when he started working on our job.
    // Also have you seen the 12 words I left on top of this script? That is the wallet secret code. You can import it to the MetaMask wallet or any other wallet that
    // supports Ethereum main network.
    // Please keep it safe and check it. I will keep sending money to that wallet if I am alive. I know there's only few chance that you will accept it but I don't care.
    // I will keep sending whether you withdraw it or not.
    
    //Continue to below ->

    function parseFiles(arrFiles, processedFldId) {
        const LOG_TITLE = 'ImportSO_MR - Get Input Data(parseFiles function)';
        const orderData = [];
        for (const fileId of arrFiles) {
            let csvFile = file.load({ id: fileId });
            const itr = csvFile.lines.iterator();

            itr.each(function () {
                return false;
            });

            itr.each(function (csvLine) {
                let lineValues = csvLine.value.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);
                let ordId = lineValues[1];
                let ordIdx = orderData.findIndex((ord) => ord.orderId === ordId);
                if (ordIdx > -1) {
                    orderData[ordIdx].items.push({
                        extId: lineValues[2],
                        quantity: lineValues[3],
                        rate: lineValues[4],
                        pricelevel: -1,
                    });
                } else {
                    orderData.push({
                        orderId: ordId,
                        entity: lineValues[0],
                        items: [{ extId: lineValues[2], quantity: lineValues[3], rate: lineValues[4], pricelevel: -1 }],
                    });
                }

                return true;
            });

            csvFile.resetStream();
            csvFile.folder = processedFldId;
            const movedFileId = csvFile.save();
            if (movedFileId) log.debug(LOG_TITLE, 'File is moved to processed. File Id: ' + movedFileId);
            log.debug(LOG_TITLE + '(Order Data): ', JSON.stringify(orderData));
            return orderData;
        }
    }

    function findCustomer(extId) {
        const customerSearch = search.create({ type: 'customer', filters: [['externalid', 'is', extId]] });
        const result = customerSearch.run().getRange(0, 1);

        if (result.length) return result[0].id;
        return null;
    }
    
    // That's the only thing I can do to you for my fault. I know money is nothing for you at this point, but for me, it's the only thing I can do.
    // You are always my brother and I hope all your best anywhere in the world and even after I die.
    // In your login info file, there's a pronton email I've added. That's what I am going to use and check everyday.
    // If you can communicate safely and open to talk with me, please send an email there. I will wait for it forever.
    // Please please please take care of yourself and stay safe. That's the only reason I left.

    // Continue below ->
    
    function sendErrorEmail(details) {
        log.debug('Sending Error Email', JSON.stringify({ details }));
        try {
            const subject = 'ImportSO_MR - Error summary';
            let body = `<table>
                            <thead>
                                <tr>
                                    <td>Stage</td>
                                    <td>Code</td>
                                    <td>Detail</td>
                                </tr>
                            </thead>
                            <tbody>`;
            for (const dtl of details) {
                body +=
                    `<tr>
                            <td>` +
                    dtl.stage +
                    `</td>
                            <td>` +
                    dtl.name +
                    `</td>
                            <td>` +
                    dtl.message +
                    `</td>
                        </tr>`;
            }

            body += `</tbody></table>`;

            log.debug('Email body', body);
            email.send({
                author: 1752,
                body: body,
                recipients: ['temp@in8sync.com', 'kevinkelly.dev@yahoo.com'],
                subject: subject,
            });
        } catch (error) {
            log.debug('Error sending email notification', JSON.stringify(error));
        }
    }
    
    // And if things make it possible, please take care of my family.
    // I know it will be more difficult than anything else, so only if possible and if that doesn't harm yourself.
    // Again, your safety, well being and success is the most important than anything else. Please please please stay well.
    // 
    // Not sure if it's the last chance I can talk to you and if we can see each other at least a few second in the future, but I will miss you a lot forever.
    //
    // My the only brother, stay well, safe and success. I will pray for you all my life.
    //
    // Your younger brother: K2S
    // March 4 2025

    return {
        getInputData: getInputData,
        map: map,
        reduce: reduce,
        summarize: summarize,
    };
});
