/**
 * ui-router service.  Controllers that link/navigate to other controllers/pages use this service.
 * See the corresponding name references in app.js
 */
angular.module(MODULE_FEED_MGR).factory('ElasticSearchService', function ($q,$http,RestUrlService) {

    function  performSearch(query, rowsPerPage,start) {
       return $http.get(RestUrlService.ELASTIC_SEARCH_URL,{params:{q:query,rows:rowsPerPage,start:start}}).then(function (response) {
             return response.data;

        });
    }



    var data = {
        searchQuery:'',
        search: function (query,rows,start) {
           return performSearch(query,rows,start);
        }

    };

    return data;



});