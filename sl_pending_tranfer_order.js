/**
 * @NApiVersion 2.x
 * @NScriptType Suitelet
 * @NModuleScope SameAccount
 */
define(['N/https', '../lib/google-auth', 'N/runtime', 'N/url', "N/file", "N/record", "N/search", 'N/redirect',"/SuiteScripts/BtjCustomization/domain/login-helper"], function (https, gAuth, runtime, url, file, record, search, redirect,loginHelper) {
    var cookieTokenKey = 'oAuthTokenId';
    /**
     * Definition of the Suitelet script trigger point.
     *
     * @param {Object} context
     * @param {ServerRequest} context.request - Encapsulation of the incoming request
     * @param {ServerResponse} context.response - Encapsulation of the Suitelet response
     * @Since 2015.2
     */
    function execute(context) {
        if (isCheckedLogin(context)) {
            showTransferOrderPage(context);
        }
        else {
            showLoginPage(context);
        }
    }
    function isCheckedLogin(context) {
        var isChecked
        var getAllEmail = getAllEmailStore();
        var isCheckCookie = loginHelper.isCheckCookie(context);
        if (isCheckCookie) {
            return true;
        }
        var urlGoogle = loginHelper.urlGoogle();
        var googleAuthorCode = loginHelper.signedGoogle(context);
        {
            if (!googleAuthorCode) {
                return false;
            }
        }
        var accountGoogleInfo = loginHelper.getGoogleAccountFromCode(googleAuthorCode,context);
        isChecked = containsEmail(getAllEmail, "email", accountGoogleInfo.email);
        if (isChecked===true) {
            var cookiesSetEmailLogin = "EmailLoginID" + "=" + accountGoogleInfo.email + ";path=/";
            context.response.setHeader("Set-Cookie", cookiesSetEmailLogin);
            var cookieNameSet = cookieTokenKey + "=" + accountGoogleInfo.tokenID + ";path=/";
            context.response.setHeader("Set-Cookie", loginHelper.encode64(cookieNameSet));
            return true;
        }
        return false;
    }
    function showLoginPage(context) {
        var urlGoogle = loginHelper.urlGoogle();
        context.response.write('<h1><a href="' + urlGoogle + '">Vui lòng đăng nhập với email Store Manager BTJ  </a></h1>');
    }
    function getAllLocation() {
        var arrayLocation = [];
        var filters =
            [
                {
                    name: "isinactive",
                    operator: "is",
                    values: [
                        "F"
                    ],

                },
                {
                    name: "subsidiary",
                    operator: "anyof",
                    values: [
                        "2"
                    ]

                },
                // {
                //     name: "email",
                //     join: "custrecord_btj_loc_manager",
                //     operator: "is",
                //     values: [
                //         "haunp@btj.vn"
                //     ],

                // }
            ];
        var columns = ["name", {
            name: "email",
            join: "CUSTRECORD_BTJ_LOC_MANAGER",
            label: "Email",
            type: "email",
            sortdir: "NONE"
        }];
        var searchLocation = search.create({
            type: search.Type.LOCATION,
            filters: filters,
            columns: columns
        });
        searchLocation.run().each(function (result) {
            var custrecord_erply_location_id = result.getValue('custrecord_erply_location_id');
            var name = result.getValue('name');
            var email = result.getValue(columns[1]);
            // if(email==="haunp@btj.vn"){
            //     arrayLocation.push({
            //         id: result.id,
            //         name: name,
            //         email:email

            //     });

            // }   
            arrayLocation.push({
                id: result.id,
                name: name,
                email: email

            });


            return true;
        });

        function isSubLocation(location) {
            var exception = "BTG - HO : BTG - Replenishment";
            if (location === exception) return false;
            return location.indexOf(":") >= 0;
        }

        function isFilterLocation(element) {
            if (isSubLocation(element.name)) {
                return false;
            }
            return true;
        }
        var newLocation = arrayLocation.filter(isFilterLocation);
        log.debug("location", newLocation);
        return newLocation;
    }
    function searchTransferOrderByLocation(idLocation) {
        var arrayTransferOrder = [];

        // var filters =[ 
        //         ['type', 'anyof', 'TrnfrOrd'],'and',
        //         ['status','noneof',['TrnfrOrd:H','TrnfrOrd:G','TrnfrOrd:C']],'and',
        // 		['mainline','is','T'],'and',
        // 		['location','anyof',parseInt(idLocation)]
        //         ];

        var filters = [
            [['type', search.Operator.ANYOF, 'TrnfrOrd'], 'and',
            ['status', 'noneof', ['TrnfrOrd:H', 'TrnfrOrd:G', 'TrnfrOrd:C']], 'and',
            ['mainline', 'is', 'T']], 'and',
            [['location', search.Operator.ANYOF, idLocation], 'or', ['transferlocation', search.Operator.ANYOF, idLocation]]];

        var columns = ["tranid", "entity", "location", "transferlocation", "status", "trandate"];
        var searchTransferOrderByLocation = search.create({
            type: search.Type.TRANSFER_ORDER,
            filters: filters,
            columns: columns
        });
        searchTransferOrderByLocation.run().each(function (result) {
            var tranid = result.getValue('tranid');
            var idInternal = result.id;
            arrayTransferOrder.push({
                tranferOrderId: tranid,
                idInternal: idInternal,
                id: result.id,
                fromLocation: result.getText('location'),
                tolocation: result.getText('transferlocation'),
                status: result.getText('status'),
                date: result.getValue('trandate'),
            });
            return true;
        });
        return arrayTransferOrder;
    }

    function getSubListOfTransferOrder(transferOrderId) {
        var detailTransferOrder = record.load({
            type: record.Type.TRANSFER_ORDER,
            id: transferOrderId,
            isDynamic: false
        });
        var getLineSubListItem = detailTransferOrder.getLineCount('item');
        var subItemTransferOrder = [];
        var itemsWithStatus = {};
        for (var i = 0; i < getLineSubListItem; i++) {
            var itemName = detailTransferOrder.getSublistText({
                sublistId: 'item',
                fieldId: 'item',
                line: i
            });
            var qtyCommitted = detailTransferOrder.getSublistValue({
                sublistId: 'item',
                fieldId: 'quantitycommitted',
                line: i
            });
            var qtyFulfilled = detailTransferOrder.getSublistValue({
                sublistId: 'item',
                fieldId: 'quantityfulfilled',
                line: i
            });
            var qtyReceived = detailTransferOrder.getSublistValue({
                sublistId: 'item',
                fieldId: 'quantityreceived',
                line: i
            });

            var isclosed = detailTransferOrder.getSublistValue({
                sublistId: 'item',
                fieldId: 'isclosed',
                line: i
            });
            itemsWithStatus[i] = {
                quantityCommitted: qtyCommitted,
                quantityFulfilled: qtyFulfilled,
                quantityReceived: qtyReceived,
                isclosed: isclosed,
                status: "not ready"
            }
            if (itemsWithStatus[i].isclosed === true) {
                itemsWithStatus[i].status = 'closed';
            } else if (itemsWithStatus[i].quantityCommitted > 0) {
                itemsWithStatus[i].status = 'ready to fulfill';
            } else if (itemsWithStatus[i].quantityReceived > 0) {
                itemsWithStatus[i].status = 'received'
            } else if (itemsWithStatus[i].quantityFulfilled > 0) {
                itemsWithStatus[i].status = 'fulfilled, waiting for receipt';
            }
            subItemTransferOrder.push({
                itemName: itemName,
                status: itemsWithStatus[i].status
            });
        }
        return subItemTransferOrder;
    }
    function showTransferOrderPage(context) {
        //
        var params = context.request.parameters;
        var action = params.action;
        switch (action) {

            case 'search_transfer_order_by_locations':
                var searchTransferOrderByLocationName = searchTransferOrderByLocation(params.idLocation);
                context.response.setHeader({
                    name: 'Content-Type',
                    value: 'application/json; charset=utf-8'
                });
                context.response.write({
                    output: JSON.stringify(searchTransferOrderByLocationName)
                });
                break;
            case 'get_sub_list_of_transfer_order':
                var transferOrderById = getSubListOfTransferOrder(params.transferOrderId);
                context.response.setHeader({
                    name: 'Content-Type',
                    value: 'application/json; charset=utf-8'
                });
                context.response.write({
                    output: JSON.stringify(transferOrderById)
                });
                break;
            case 'get_location':
                var allLocation = getAllLocation();
                context.response.setHeader({
                    name: 'Content-Type',
                    value: 'application/json; charset=utf-8'
                });
                context.response.write({
                    output: JSON.stringify(allLocation)
                });
                break;
            default:
                var html = file.load(5027089).getContents();
                context.response.write({
                    output: html
                });
                break;

        }
    }
    function getAllEmailStore() {
        var emailArray = [];
        var filters = [];
        columns = [
            {
                name: "email",
                join: "CUSTRECORD_BTJ_LOC_MANAGER",
                label: "Email",
                type: "email",
                sortdir: "NONE"
            }]
        var searchEmailStore = search.create({
            type: search.Type.LOCATION,
            filters: filters,
            columns: columns
        });
        searchEmailStore.run().each(function (result) {
            var email = result.getValue(columns[0]);
            emailArray.push({
                email: email
            });
            return true;

        });
        //add email haunp@btj.vn for test 
        emailArray.push({ "email": "haunp@btj.vn" });
        return emailArray;
    }
    function containsEmail(arr, key, val) {
        for (var i = 0; i < arr.length; i++) {
            if (arr[i][key] === val) return true;
        }
        return false;
    }
    return {
        onRequest: execute
    };
});


