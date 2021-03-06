var taskerDashboard = angular.module(
    "taskerDashboard",
    [
        "ngRoute",
    ]
);
taskerDashboard.config(
    [
        "$routeProvider",
        "$locationProvider",
        function($routeProvider, $locationProvider) {
            $locationProvider
            .hashPrefix("!");

            $routeProvider
            .when(
                "/dashboard",
                {
                    templateUrl: "pages/dashboard.html"
                }
            )
            .otherwise(
                {
                    redirectTo: "dashboard"
                }
            );
        }
    ]
);

taskerDashboard.controller(
    "dashboardController",
    [
        "$scope",
        "$location",
        "$interval",
        "$timeout",
        function dashboardController($scope, $location, $interval, $timeout) {
            var domain = location.hostname + (location.port ? ":" + location.port: "");
            var websocket = new WebSocket("ws://" + domain + "/ws/statistics");

            $scope.websocketConnected = false;
            $scope.metrics = {
                "process": 0,
                "success": 0,
                "retry": 0,
                "failure": 0
            };
            $scope.rates = {
                "process_per_second": 0,
                "success_per_second": 0,
                "retry_per_second": 0,
                "failure_per_second": 0
            };
            $scope.statistics = [];
            $scope.workers = [];
            $scope.queues = [];

            $scope.workersTableSortBy = "hostname";
            $scope.workersTableSortByReverse = true;
            $scope.statisticsTableSortBy = "worker_name";
            $scope.statisticsTableSortByReverse = true;
            $scope.queuesTableSortBy = "queue_name";
            $scope.queuesTableSortByReverse = true;

            $scope.updateWorkers = function() {
                websocket.send("workers");
            };

            websocket.onclose = function(event) {
                $scope.websocketConnected = false;
            };
            websocket.onopen = function(event) {
                $scope.websocketConnected = true;
                websocket.send("metrics");
                websocket.send("queues");
                websocket.send("statistics");
            };
            websocket.onmessage = function(event) {
                var message = JSON.parse(event.data);

                if (message.type === "metrics") {
                    $scope.metrics = message.data.metrics;
                    $scope.rates = message.data.rates;

                    $timeout(
                        function() {
                            websocket.send("metrics");
                        },
                        1000
                    );
                } else if (message.type === "queues") {
                    var queues = [];

                    for (var key in message.data) {
                        queues.push(
                            {
                                'queue_name': key,
                                'queue_count': message.data[key]
                            }
                        );
                    }

                    $scope.queues = queues;

                    $timeout(
                        function() {
                            websocket.send("queues");
                        },
                        2000
                    );
                } else if (message.type === "statistics") {
                    $scope.statistics = message.data;

                    $timeout(
                        function() {
                            websocket.send("statistics");
                        },
                        3000
                    );
                } else if (message.type === "workers") {
                    $scope.workers = message.data;
                }
            };
        }
    ]
);
