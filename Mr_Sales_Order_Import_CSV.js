/**
 * MR_Sales_Order_Import_HauNguyen.js
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */

/*
 * Author: Jay Nguyen
 * Date: 23 Nov 2021
 *
 * Description: This script processes only files 'in8_soimport_jaynguyen'.
 */
define([
    "N/email",
    "N/error",
    "N/file",
    "N/record",
    "N/runtime",
    "N/search",
],
    /**
     * @param {email} email
     * @param {error} error
     * @param {file} file
     * @param {record} record
     * @param {runtime} runtime
     * @param {search} search
     */
    function (email, error, file, record, runtime, search) {
        const CONFIG_SOIMPORT = {
            FOLDER_PENDING: 873,
            FOLDER_PROCESSED: 874,
            FILE_PARSER: {
                SEPARATOR: /,/gm,
                COLUMNS: {
                    ORDER_ID: 0,
                    ENTITY_ID: 1,
                    ITEM_ID: 2,
                    ITEM_QTY: 3,
                    ITEM_RATE: 4,
                    ITEM_AMT: 5,
                },
            },
            CUSTOM_PRICE_LEVEL: "-1",
        };

        /**
         * Marks the beginning of the Map/Reduce process and generates input data.
         *
         * @typedef {Object} ObjectRef
         * @property {number} id - Internal ID of the record instance
         * @property {string} type - Record type id
         *
         * @return {Array|Object|Search|RecordRef} inputSummary
         * @since 2015.1
         */
        function getInputData() {
            var logName = "GET INPUT DATA";
            log.audit(logName, "----- Start -----");
            try {
                var searchFile = search.create({
                    type: "file",
                    filters: [
                        ["folder", "anyof", CONFIG_SOIMPORT.FOLDER_PENDING],
                        "AND",
                        ["name", "contains", "in8_soimport_jaynguyen"],
                    ],
                });
                var countFile = searchFile.runPaged().count;
                log.debug("countFile", countFile);

                if (countFile < 1) {
                    log.audit("NO_FILES_TO_PROCESS", "Dont have files in PENDING folder");
                }
                return searchFile;
            } catch (error) {
                log.error(logName + " - UNEXPECTED_ERROR", error);
            }
        }

        /**
         * Executes when the map entry point is triggered and applies to each key/value pair.
         *
         * @param {MapSummary} context - Data collection containing the key/value pairs to process through the map stage
         * @since 2015.1
         */
        function map(context) {
            var logName = "MAP";
            log.debug(logName, "----- Start -----");
            log.debug("context.key", context.key);
            log.debug("context.value", context.value);

            var filedId = context.key;

            var csvFileObj = file.load({
                id: filedId,
            });

            var fileIterator = csvFileObj.lines.iterator();

            // Skip first header line
            fileIterator.each(function () {
                return false;
            });
            var arrCSVLines = [];
            fileIterator.each(function (line) {
                var lineValues = line.value.split(CONFIG_SOIMPORT.FILE_PARSER.SEPARATOR);
                var lineValuesDupe = lineValues;
                lineValuesDupe.filter(function (value) {
                    return !isEmpty(value);
                });
                if (isEmpty(lineValuesDupe)) {
                    return true;
                }
                arrCSVLines.push(lineValues);
                return true;
            });
            log.debug("arr CSV Lines", arrCSVLines);
            for (var i = 0; i < arrCSVLines.length; i++) {
                var arrColValues = arrCSVLines[i];
                var strOrderId =arrColValues[CONFIG_SOIMPORT.FILE_PARSER.COLUMNS.ORDER_ID] || "";
                var strEntityExtId =arrColValues[CONFIG_SOIMPORT.FILE_PARSER.COLUMNS.ENTITY_ID] || "";
                var strItemId =arrColValues[CONFIG_SOIMPORT.FILE_PARSER.COLUMNS.ITEM_ID] || "";
                var intItemQty =arrColValues[CONFIG_SOIMPORT.FILE_PARSER.COLUMNS.ITEM_QTY] || 0;
                var flItemRate =arrColValues[CONFIG_SOIMPORT.FILE_PARSER.COLUMNS.ITEM_RATE] || 0;
                var flItemAmount =arrColValues[CONFIG_SOIMPORT.FILE_PARSER.COLUMNS.ITEM_AMT] || 0;

                if (isEmpty(strEntityExtId) ||isEmpty(strOrderId) ||isEmpty(strItemId)) {
                    continue;
                }

                var salesOrderObj = {
                    customer: strEntityExtId,
                    itemid: strItemId,
                    itemqty: intItemQty,
                    itemrate: flItemRate,
                    itemamount: flItemAmount,
                };

                var key = filedId + "#" + strOrderId;
                log.debug("Key", key);
                log.debug("salesOrderObj", salesOrderObj);

                context.write(key, salesOrderObj);
            }
        }

        /**
         * Executes when the reduce entry point is triggered and applies to each group.
         *
         * @param {ReduceSummary} context - Data collection containing the groups to process through the reduce stage
         * @since 2015.1
         */
        function reduce(context) {
            var logName = "REDUCE";
            var customerExtId = "";
            var arrItemNames = [];
            var arrItemLines = [];
            log.debug(logName, "----- Start -----");
            var csvFileId = context.key.split("#")[0];
            var salesOrderId = context.key.split("#")[1];
            log.debug("csvFileId", csvFileId);
            log.debug("salesOrderId", salesOrderId);
            context.values.forEach(function (params) {
                var dataObj = JSON.parse(params);
                customerExtId = dataObj["customer"];
                var itemName = dataObj["itemid"];
                var quantity = parseInt(dataObj["itemqty"]) || 1;
                var itemRate = parseFloat(dataObj["itemrate"]) || 0;
                var flAmount = parseFloat(dataObj["itemamount"]) || 0;
                if (arrItemNames.indexOf(itemName) == -1) {
                    arrItemNames.push(itemName);
                }

                var lineData = {
                    itemid: itemName,
                    itemqty: quantity,
                    itemrate: itemRate,
                    itemamount: flAmount,
                };

                arrItemLines.push(lineData);
            });
            var customerId = getCustomerInternalId(customerExtId);
            log.debug("customerId", customerId);
            var itemDataObj = getItemsInternalId(arrItemNames);
            log.debug("itemDataObj", itemDataObj);
            var recId = createSalesOrder(salesOrderId,customerId,arrItemLines,itemDataObj)
            log.audit("Sales Order created", recId);
            context.write(csvFileId, recId);

        }

        /**
         * Executes when the summarize entry point is triggered and applies to the result set.
         *
         * @param {Summary} summary - Holds statistics regarding the execution of a map/reduce script
         * @since 2015.1
         */
        function summarize(summary) {
            var logName = "SUMMARIZE";
            log.debug(logName, "----- Start -----");

            handleError(summary);

            var arrProcessedFiles = [];
            var numberSoCreated = 0;
            summary.output.iterator().each(function (key, value) {
                var csvFileId = key;

                if (arrProcessedFiles.indexOf(csvFileId) == -1) {
                    arrProcessedFiles.push(csvFileId);
                }

                numberSoCreated++;
                return true;
            });
            log.audit(logName, numberSoCreated + " Sales Order created");

            log.debug(logName, "Moving files to PROCESSED folder");

            for (var i in arrProcessedFiles) {
                var csvFileObj = file.load({
                    id: arrProcessedFiles[i],
                });

                csvFileObj.folder = CONFIG_SOIMPORT.FOLDER_PROCESSED;
                var newCsvFileId = csvFileObj.save();
                log.debug("newCsvFileId", newCsvFileId);
            }
            log.debug(logName, "----- End -----");
        }

        function handleError(summary) {
            log.debug("handleError");

            var inputSummary = summary.inputSummary;
            var mapSummary = summary.mapSummary;
            var reduceSummary = summary.reduceSummary;

            if (inputSummary.error) {
                createError({
                    name: "INPUT_STAGE_FAILED",
                    message: inputSummary.error,
                    stage: "getInputData",
                });
            }

            handleErrorInStage("map", mapSummary);
            handleErrorInStage("reduce", reduceSummary);
        }

        function handleErrorInStage(stage, summary) {
            log.debug("handleErrorInStage",stage);

            var arrErrorMsg = [];
            summary.errors.iterator().each(function (key, value) {
                var strMsg = "";
                strMsg = "Error: " + value;
                arrErrorMsg.push(strMsg);

                return true;
            });

            if (arrErrorMsg.length > 0) {
                createError({
                    name: stage.toUpperCase() + "_STAGE_FAILED",
                    message: JSON.stringify(arrErrorMsg),
                    stage: stage,
                });
            }
        }

        function createError(paramsObj) {
            log.debug("createError");

            var errorObj = error.create({
                name: paramsObj.name,
                message: paramsObj.message,
                notifyOff: false,
            });
            log.error(errorObj.name, errorObj.message);
            handleErrorAndSendNotification(errorObj, paramsObj.stage);

            return errorObj;
        }

        function handleErrorAndSendNotification(e, stage) {
            var intAuthor = -5; 
            var strRecipients = "nguyenjay90@gmail.com";
            var strSubject =
                "Map/Reduce script " +
                runtime.getCurrentScript().id +
                " failed for stage: " +
                stage;
            var strBody =
                "An error occurred with the following information:\n" +
                "Error code: " +
                e.name +
                "\n" +
                "Error msg: " +
                e.message;

            email.send({
                author: intAuthor,
                recipients: strRecipients,
                subject: strSubject,
                body: strBody,
            });
        }

        function isEmpty(strValue) {
            try {
                return (
                    strValue === "" ||
                    strValue == null ||
                    strValue == undefined ||
                    (strValue.constructor === Array && strValue.length == 0) ||
                    (strValue.constructor === Object &&
                        (function (v) {
                            for (var k in v) return false;
                            return true;
                        })(strValue))
                );
            } catch (e) {
                return true;
            }
        }

        function createSalesOrder(salesOrderId,customerId,arrItemLines,itemDataObj) {
            var rec = record.create({
                type: record.Type.SALES_ORDER,
                isDynamic: true,
            });
            rec.setValue({
                fieldId: "tranid",
                value: salesOrderId,
            });
            rec.setValue({
                fieldId: "entity",
                value: customerId,
            });
            rec.setValue({
                fieldId: "memo",
                value: "This is test",
            });
            for (var intLine = 0; intLine < arrItemLines.length; intLine++) {
                rec.selectNewLine({
                    sublistId: "item",
                });

                var itemId = itemDataObj[arrItemLines[intLine]["itemid"]];
                rec.setCurrentSublistValue({
                    sublistId: "item",
                    fieldId: "item",
                    value: itemId,
                });

                rec.setCurrentSublistValue({
                    sublistId: "item",
                    fieldId: "quantity",
                    value: arrItemLines[intLine]["itemqty"],
                });

                rec.setCurrentSublistValue({
                    sublistId: "item",
                    fieldId: "price",
                    value: CONFIG_SOIMPORT.CUSTOM_PRICE_LEVEL,
                });

                rec.setCurrentSublistValue({
                    sublistId: "item",
                    fieldId: "rate",
                    value: arrItemLines[intLine]["itemrate"],
                });

                rec.setCurrentSublistValue({
                    sublistId: "item",
                    fieldId: "amount",
                    value: arrItemLines[intLine]["itemamount"],
                });

                rec.commitLine({
                    sublistId: "item",
                });
            }

            var recId = rec.save({
                enableSourcing: true,
                ignoreMandatoryFields: false,
            });
            return recId;
        }

        function getCustomerInternalId(strCustExtId) {
            var customerSearchObj = search.create({
                type: "customer",
                filters: [
                    ["externalid", "anyof", strCustExtId.toUpperCase()],
                    "AND",
                    ["isinactive", "is", "F"],
                ],
                columns: [
                    search.createColumn({
                        name: "internalid",
                        sort: search.Sort.ASC,
                    }),
                    "entityid",
                ],
            });

            var customerSearchObjCount = customerSearchObj.runPaged().count;
            if (customerSearchObjCount < 1) {
                var errorObj = {
                    name: "NO_CUSTOMER_FOUND",
                    message: "No Customer with External ID: " + strCustExtId + " found",
                };

                throw errorObj;
            }

            var custInternalId = 0;
            customerSearchObj.run().each(function (result) {
                custInternalId = result.id;
                return true;
            });

            return custInternalId;
        }

        function getItemsInternalId(arrItemIds) {
            var arrItemFilters = [];
            for (var i = 0; i < arrItemIds.length; i++) {
                arrItemFilters.push(["name", "is", arrItemIds[i]]);

                if (i < arrItemIds.length - 1) {
                    arrItemFilters.push("OR");
                }
            }
            arrItemFilters.push("AND");
            arrItemFilters.push(["isinactive", "is", "F"]);
            log.debug("arrItemFilters", arrItemFilters);

            var itemSearchObj = search.create({
                type: "item",
                filters: arrItemFilters,
                columns: [
                    search.createColumn({
                        name: "internalid",
                        sort: search.Sort.ASC,
                    }),
                    "itemid",
                ],
            });

            var itemSearchObjCount = itemSearchObj.runPaged().count;
            log.debug("itemSearchObjCount", itemSearchObjCount);

            if (itemSearchObjCount < 1) {
                var errorObj = {
                    name: "NO_ITEMS_FOUND",
                    message: "No Items with Item IDs: " + arrItemIds.toString() + " found",
                };

                throw errorObj;
            }

            var itemObj = {};
            itemSearchObj.run().each(function (result) {
                var strItemName = result.getValue({
                    name: "itemid",
                });

                var intItemId = result.id;

                if (!itemObj.hasOwnProperty(strItemName)) {
                    itemObj[strItemName] = intItemId;
                }

                return true;
            });

            return itemObj;
        }

        return {
            getInputData: getInputData,
            map: map,
            reduce: reduce,
            summarize: summarize,
        };
    });
