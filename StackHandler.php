<?php
include_once __DIR__ . '/config.php';

class StackHandler
{
    protected $stackId;
    protected $clid;
    protected $apiKey;
    protected $originPath;

    public function __construct(int $clid, string $stackId, string $enName, string $requestType, string $originPath = null)
    {
        $date = date('d-m-Y');

        HU::logSetup(__DIR__ . "/logs/$enName/handler_{$stackId}_$date.log");

        $this->stackId = $stackId;
        $this->clid = $clid;
        $this->apiKey = key_Get($clid);
        $this->originPath = $originPath;
        $this->message('----HANDLER-START----');
        $this->message("Тип запроса: $requestType");
    }

    public function setOriginalLogPath()
    {
        $this->message('----HANDLER-END----');

        if ($this->originPath) {
            HU::logSetup($this->originPath);
        }

        return true;
    }

    /**
     * @param array $openElements
     * @return bool
     * @throws Exception
     */
    public function run(array $openElements = null)
    {
        $this->message('Запущено распределение пользователей');
        $this->message("Получаем непринятые элементы для очереди [ {$this->stackId} ] из БД");

        $openElements = $openElements ?? $this->getStackOpenElements();

        if (!$openElements) {
            $this->message('Элементов нет, выходим');

            return true;
        }

        $this->message('Проверка на флаг "в обработке" в очереди');

        $elementInUse = $this->findElementInUse($openElements);
        $elementId = $elementInUse['element_id'] ?? null;
        $status = $elementInUse['element_status'] ?? null;

        if ($elementId && !$status) {
            $this->message('Ошибка нахождения статуса для элемента:');
            $this->message($elementInUse);
        }

        if ($elementInUse && $status == 'sended') {
            $this->message('Есть элемент в обработке со статусом sended:');
            $this->message($elementInUse);

            $inUseUpdated = $elementInUse['in_use'];
            $inUseCreated = $elementInUse['in_use_created'];
            $time5m = time() - 60 * 5;
            $time24h = time() - 60 * 60 * 24;
            $message = 'Метка времени для флага устарела на 5 минут: ';
            $condition = $inUseUpdated < $time5m;

            if ($condition) {
                $this->message($message . 'да');
                $newStatus = '';
                $comment = '5 min';

                if ($inUseCreated < $time24h) {
                    $this->message('С даты создания флага прошло 24 часа');

                    $newStatus = 'accepted';
                    $comment = '24 hours';

                    $this->createComment($elementId, 'Сделка в распределении больше 24 часов', 'Убер');
                }

                $this->message('Убираем флаг "в обработке"');
                $this->changeElement($elementId, false, $newStatus, $comment);

                // Метка времени для флага устарела на 5 минут - NO
            } else {
                $this->message($message . 'нет');
            }

            $this->message('Выходим');

            return true;
        }

        $onlineStackUsers = $this->getOnlineStackUsers();

        if ($onlineStackUsers) {
            $this->message('Есть онлайн пользователи');

            if (!$elementInUse) {
                $this->message('Нет элемента в обработке, находим самый старый элемент:');
                $elementInUse = $this->getOldestElement($openElements);

                $this->createComment($elementInUse['element_id'], 'Начато распределение');
            } else {
                $this->message('Есть элемент в обработке со статусом refused:');
            }

            $this->message($elementInUse);
        } else {
            $this->message('Нет онлайн пользователей, выходим');

            return true;
        }

        $this->handleUsersAndSendElement($onlineStackUsers, $elementInUse);

        return true;
    }

    /**
     * @param array $onlineStackUsers
     * @param array $elementInUse
     * @return bool
     * @throws Exception
     */
    protected function handleUsersAndSendElement(array $onlineStackUsers, array $elementInUse)
    {
        $this->message('Обработка пользователей и отправка элемента');

        $usersWithoutSendFlag = $this->getUsersWithoutSendFlag($onlineStackUsers);
        $elementId = $elementInUse['element_id'];

        if (!$usersWithoutSendFlag) {
            $this->message('У всех пользователей есть отметка об отправке, убираем ее');
            $this->removeSendFlagForAll($this->getStackUsers());

            $usersWithoutSendFlag = $onlineStackUsers;
        }

        $usersWithoutSendFlag = $this->sortUsers($usersWithoutSendFlag);

        $this->message('Отсортированные пользователи без метки');
        $this->message($usersWithoutSendFlag);

        $currentUser = reset($usersWithoutSendFlag);
        $userId = $currentUser['user_id'];

        $this->message('Первый пользователь');
        $this->message($currentUser);

        if (time() > $currentUser['refuse_time']) {
            $this->message("Метка об отказе просрочена, отправляем элемент $elementId пользователю $userId");

            $this->sendElementToUser($elementInUse, $userId);
            $this->message('Элемент успешно отправлен');

            $this->message("Ставим пользователю $userId отметку об отправке");
            $this->addSendFlagToUser($userId, $currentUser['sended'] ?? 0);

            $this->message("Ставим элементу $elementId флаг в обработке и статус sended");
            $this->changeElement($elementId, true, 'sended', null, true);

            return true;
        }

        $this->message('Метка об отказе актуальна, проверяем остальных пользователей');

        $onlineStackUsers = $this->getOnlineStackUsers();

        $this->message('убираем текущего пользователя');

        unset($onlineStackUsers[$userId]);

        $this->message('убираем всех с актуальными метками об отказе');

        $onlineStackUsers = array_filter($onlineStackUsers, function ($user) {
            return time() > $user['refuse_time'];
        });

        if (!$onlineStackUsers) {
            $this->message('Никого не осталось, выходим');

            return true;
        }

        $this->message('Онлайн пользователи с просроченными метками об отказе');
        $this->message($onlineStackUsers);

        return $this->handleUsersAndSendElement($onlineStackUsers, $elementInUse);
    }

    /**
     * @param array $element
     * @param int $userId
     * @return bool
     * @throws Exception
     */
    protected function sendElementToUser(array $element, int $userId): bool
    {
        $msg = [
            'action' => 'sendToUserFirstConnection',
            'user_id' => $userId,
            'first_connection_data' => [
                'event' => 'uber_new_element',
                'elementName' => $element['element_name'],
                'elementId' => $element['element_id'],
                'elementType' => $element['element_type'],
                'contactId' => json_decode($element['element_data'], 1)['contact_id'],
                'stackId' => $this->stackId,
                'users' => [$userId],
                'playSound' => true
            ],
            'event' => 'uber_new_element',
            'elementName' => $element['element_name'],
            'elementId' => $element['element_id'],
            'elementType' => $element['element_type'],
            'contactId' => json_decode($element['element_data'], 1)['contact_id'],
            'stackId' => $this->stackId,
            'users' => [$userId],
            'playSound' => true,
        ];

        return $this->sendToUberSocket($msg);
    }

    /**
     * @param $data
     * @return bool
     * @throws Exception
     */
    protected function sendToUberSocket(array $data): bool
    {
        $wsUrl = "ws://localhost:8282/main?key={$this->apiKey}";
        $errorTry = 0;
        $next = true;

        $this->message("Отправляем данные на сокет сервер [ $wsUrl ]:");
        $this->message($data);

        do {
            $errorTry++;

            try {
                $client = new WebSocket\Client($wsUrl);
                $client->send(json_encode($data));
                $client->close();
                $next = false;
            } catch (Exception $e) {
                $this->message('Произошла ошибка во время отправки заявки на вебсокет сервер:');
                $this->message($e->getMessage());
            }
        } while ($next && $errorTry < 3);

        if ($errorTry >= 3 && $next) {
            $this->error('Не удалось отправить заявку на убер', $e);
        }

        return true;
    }

    /**
     * Рассчитываем очередность пользователей,
     * по отношению количество принятых заявок / вес
     * @param $users
     * @return mixed
     */
    protected function sortUsers(array $users): array
    {
        usort($users, function ($a, $b) {
            $countElementsA = $a['accepted'] ?? 0;
            $weightA = !isset($a['weight']) || $a['weight'] === 0 ? 1 : $a['weight'];

            $countElementsB = $b['accepted'] ?? 0;
            $weightB = !isset($b['weight']) || $b['weight'] === 0 ? 1 : $b['weight'];

            $countA = $countElementsA / $weightA;
            $countB = $countElementsB / $weightB;

            return $countA > $countB || ($countA == $countB && $a['user_id'] > $b['user_id']);
        });

        return $users;
    }

    /**
     * @param int $userId
     * @param int $sended
     * @return bool
     * @throws Exception
     */
    protected function addSendFlagToUser(int $userId, int $sended)
    {
        return $this->updateCountsEntry($userId, true, ++$sended);
    }

    /**
     * Общий метод для изменения записи в таблице widget_uber_counts
     * @param int $userId
     * @param bool|null $sendFlag
     * @param int|null $sended
     * @param int|null $accepted
     * @param int|null $refuseTime
     * @return bool
     * @throws Exception
     */
    private function updateCountsEntry(int $userId, bool $sendFlag = null, int $sended = null, int $accepted = null, int $refuseTime = null)
    {
        $this->message('Обновляем запись в ' . $this->getCountsTable());

        // Составляем массив изменяемых параметров для запроса, убираем пустые элементы
        $argArray = array_filter([
            is_null($sendFlag) ? '' : 'send_flag=:send_flag',
            is_null($sended) ? '' : 'sended=:sended',
            is_null($accepted) ? '' : 'accepted=:accepted',
            is_null($refuseTime) ? '' : 'refuse_time=:refuse_time',
        ], function ($val) {
            return $val;
        });
        $setParams = implode(',', $argArray);
        $query = HU::setQueryParams(
        /** @lang text */
            "INSERT INTO {$this->getCountsTable()} SET user_id =:user_id, stack_id=:stack_id, clid=:clid, $setParams ON DUPLICATE KEY UPDATE $setParams",
            [
                ':send_flag' => (int)$sendFlag,
                ':sended' => $sended,
                ':accepted' => $accepted,
                ':refuse_time' => $refuseTime,
                ':user_id' => $userId,
                ':stack_id' => $this->stackId,
                ':clid' => $this->clid,
            ]
        );

        return $this->executeQuery($query);
    }

    /**
     * @param array $stackUsers
     * @return bool
     * @throws Exception
     */
    protected function removeSendFlagForAll(array $stackUsers)
    {
        $userIds = implode(',', array_values(array_column($stackUsers, 'user_id')));
        $query = HU::setQueryParams(
        /** @lang text */
            "UPDATE {$this->getCountsTable()}
                     SET send_flag=:send_flag
                     WHERE user_id in ($userIds) 
                     AND stack_id=:stack_id",
            [
                ':send_flag' => 0,
                ':stack_id' => $this->stackId,
            ]
        );

        return $this->executeQuery($query);
    }

    protected function getUsersWithoutSendFlag(array $onlineStackUsers)
    {
        return array_filter($onlineStackUsers, function ($user) {
            return !$user['send_flag'];
        });
    }

    protected function getOldestElement($openElements)
    {
        return reset($openElements);
    }

    /**
     * @param $elementId
     * @param string $text
     * @param string $service
     * @param int $elementType
     * @param int $noteType
     * @param string $comment
     * @return bool
     * @throws Exception
     */
    public function createComment($elementId, string $text, string $service = 'Убер', $elementType = 2, $noteType = 25, $comment = '')
    {
        $this->message('Создаем примечание, результат:');

        try {
            $result = amocrm_CreateComment(
                $comment,
                $elementId,
                $elementType,
                $noteType,
                null, null, null, null, null,
                [
                    'text' => $text,
                    'service' => $service,
                ]
            );

            $this->message($result);
        } catch (Exception $exception) {
            $this->message('Ошибка при создании примечания:');
            $this->message($exception);
        }

        return true;
    }

    /**
     * @return array
     * @throws Exception
     */
    public static function handleHook(): array
    {
        $incomingData = HU::request('leads');
        $type = HU::request('type');
        $leadId = null;

        if (!is_array($incomingData)) {
            throw new Exception('Некорректный формат хука:' . $incomingData);
        }

        foreach ($incomingData as $event => $value) {
            if (isset($incomingData[$event][0]['id'])) {
                $leadId = $value[0]['id'];

                break;
            }
        }

        if ($leadId) {
            $lead = HAmo::getLead($leadId);
        } else {
            throw new Exception('Не найдены данные о сделке, выходим');
        }

        return [
            'lead' => $lead,
            'lead_id' => $leadId,
            'type' => $type,
            'event' => $event,
        ];
    }

    /**
     * @param array $lead
     * @param string $type
     * @return bool
     * @throws Exception
     */
    public function handleElement(array $lead, string $type)
    {
        $this->message('Обрабатываем новый элемент:');
        $this->message($lead);

        $contactId = (isset($lead['main_contact_id']) && $lead['main_contact_id']) ? $lead['main_contact_id'] : null;
        $element = [
            'element_id' => $lead['id'],
            'element_type' => $type,
            'element_data' => json_encode(['contact_id' => $contactId], JSON_HEX_APOS),
            'element_name' => ($type === 'unsortedV6') ? 'Новая заявка' : $lead['name']
        ];
        $this->addElement($element);

        return true;
    }

    /**
     * @param array $element
     * @return bool
     * @throws Exception
     */
    protected function addElement(array $element)
    {
        $this->message('Добавляем новый элемент');

        // если такой элемент уже есть и у него статус sended,
        // то не меняем статус, чтобы не сломать очередь
        $query = HU::setQueryParams(/* @lang text */
            "INSERT INTO {$this->getElementsTable()} SET
        clid = :clid,
        stack_id = :stack_id,
        date_create = :date_create,
        date_update = :date_create,
        element_type = :element_type,
        element_id = :element_id,
        element_status = :element_status,
        element_data = :element_data,
        element_name = :element_name,
        comment = :comment
        ON DUPLICATE KEY UPDATE 
        element_status = IF(element_status = 'sended', 'sended', :element_status),
        user_id = :user_id,
        date_update = :date_create,
        element_data = :element_data,
        element_name = :element_name,
        comment = :comment",
            [
                ':clid' => $this->clid,
                ':stack_id' => $this->stackId,
                ':date_create' => time(),
                ':element_type' => $element['element_type'],
                ':element_id' => $element['element_id'],
                ':element_status' => 'new',
                ':element_data' => $element['element_data'],
                ':element_name' => $element['element_name'],
                ':user_id' => 0,
                ':comment' => '',
            ]);

        return $this->executeQuery($query);
    }

    /**
     * @param bool $inUse
     * @param int $elementId
     * @param string $newStatus
     * @param string $comment
     * @param bool $inUseCreated
     * @param int $userId
     * @return bool
     * @throws Exception
     */
    protected function changeElement(int $elementId, bool $inUse = null, string $newStatus = null, string $comment = null, bool $inUseCreated = null, int $userId = null)
    {
        // Составляем массив изменяемых параметров для запроса, убираем пустые элементы
        $argArray = array_filter([
            is_null($inUse) ? '' : 'in_use=:in_use',
            is_null($newStatus) ? '' : 'element_status=:element_status',
            is_null($inUseCreated) ? '' : 'in_use_created=:in_use_created',
            is_null($userId) ? '' : 'user_id=:user_id',
            is_null($comment) ? '' : 'comment=:comment',
        ], function ($val) {
            return $val;
        });
        $setParams = implode(',', $argArray);

        $query = HU::setQueryParams(
        /** @lang text */
            "UPDATE {$this->getElementsTable()}
                     SET $setParams
                     WHERE element_id=:element_id 
                     AND stack_id=:stack_id",
            [
                ':in_use' => $inUse ? time() : 0,
                ':element_status' => $newStatus,
                ':in_use_created' => $inUseCreated ? time() : 0,
                ':user_id' => $userId,
                ':comment' => $comment,
                ':element_id' => $elementId,
                ':stack_id' => $this->stackId,
            ]
        );

        return $this->executeQuery($query);
    }

    /**
     * @return array
     * @throws Exception
     */
    protected function getOnlineStackUsers(): array
    {
        $result = array_intersect_key($this->getStackUsers(), $this->getOnlineUsers());

        $this->message('Получаем онлайн пользователей для очереди:');
        $this->message($result);

        return $result;
    }

    /**
     * @return array
     * @throws Exception
     */
    protected function getStackUsers(): array
    {
        $query = HU::setQueryParams(/** @lang sql */
            "SELECT wuu.*, wuc.sended, wuc.accepted, wuc.send_flag, wuc.refuse_time FROM {$this->getUsersTable()} wuu
left join {$this->getCountsTable()} wuc on wuu.user_id = wuc.user_id and wuu.stack_id = wuc.stack_id
where wuu.stack_id = :stack_id",
            [':stack_id' => $this->stackId]
        );

        return $this->getQueryList($query, 'user_id');
    }

    /**
     * @return array
     * @throws Exception
     */
    protected function getOnlineUsers(): array
    {
        $query = HU::setQueryParams(/** @lang text */
            'SELECT user FROM connection_session WHERE clid = :clid AND date_closed IS NULL',
            [':clid' => $this->clid]
        );

        return $this->getQueryList($query, 'user');
    }

    protected function findElementInUse(array $openElements): array
    {
        $elementInUse = [];

        foreach ($openElements as $element) {
            if ($element['in_use']) {
                $elementInUse = $element;
                break;
            }
        }

        return $elementInUse;
    }

    /**
     * @return array
     * @throws Exception
     */
    protected function getStackOpenElements()
    {
        $query = HU::setQueryParams(/** @lang text */
            "SELECT * FROM {$this->getElementsTable()} WHERE
        `stack_id` = :stack_id AND
        `element_status` != 'accepted' ORDER BY date_create", [':stack_id' => $this->stackId]);

        return $this->getQueryList($query, 'element_id');
    }

    /**
     * @param $query
     * @param string $key
     * @return array
     * @throws Exception
     */
    protected static function getQueryList(string $query, string $key = 'id'): array
    {
        try {
            $result = getQueryList($query, $key);
        } catch (Exception $exception) {
            self::error("Запрос\n$query\nзавершился ошибкой: " . $exception->getMessage(), $exception);
        }

        return $result ?? [];
    }

    /**
     * @param string $query
     * @return array
     * @throws Exception
     */
    protected static function getQueryOne(string $query): array
    {
        try {
            $result = getQueryOne($query);
        } catch (Exception $exception) {
            self::error("Запрос\n$query\nзавершился ошибкой: " . $exception->getMessage(), $exception);
        }

        return $result ?? [];
    }

    /**
     * @param $query
     * @return bool
     * @throws Exception
     */
    protected function executeQuery($query)
    {
        try {
            executeQuery($query);
        } catch (Exception $exception) {
            $this->error("Запрос\n$query\nзавершился ошибкой: " . $exception->getMessage(), $exception);
        }

        return true;
    }

    protected static function getUsersTable()
    {
        return '`widget_uber_users`';
    }

    protected static function getElementsTable()
    {
        return '`widget_uber_elements`';
    }

    protected static function getCountsTable()
    {
        return '`widget_uber_counts`';
    }

    protected static function getLogsTable()
    {
        return '`widget_uber_logs`';
    }

    protected static function getStacksTable()
    {
        return '`widget_uber_stacks`';
    }

    public static function message($message)
    {
        HU::log($message);
    }

    /**
     * @param $message
     * @param Exception $e
     * @throws Exception
     */
    public static function error($message, Exception $e)
    {
        self::message($message);

        throw new Exception($message, $e->getCode(), $e);
    }

    /**
     * @return array
     * @throws Exception
     */
    protected static function getOpenElements(): array
    {
        $tableName = self::getElementsTable();
        $query = HU::setQueryParams(/** @lang text */
            "SELECT * FROM $tableName 
            WHERE `element_status` != 'accepted' ORDER BY date_create");

        return self::getQueryList($query, 'element_id');
    }

    /**
     * @return array
     * @throws Exception
     */
    public static function getOpenElementsByStack(): array
    {
        return array_reduce(self::getOpenElements(), function ($acc, $element) {
            $acc[$element['stack_id']][] = $element;

            return $acc;
        }, []);
    }

    /**
     * @param int $clid
     * @return string
     * @throws Exception
     */
    public static function getEnnameByClid(int $clid): string
    {
        return self::getQueryOne(/** @lang text */
            "SELECT * FROM clients WHERE clid = $clid")['enname'];
    }

    public static function checkWidget(int $clid)
    {
        $widget = \Yadro\YadroWidget::getByCode('uber');
        $widgetData = $widget->getClientData($clid);
        $widgetParams = json_decode($widgetData['params'], true);

        if (!isset($widgetParams['frontend_status']) || !$widgetParams['frontend_status']) {
            return false;
        }

        return true;
    }

    /**
     * @return array
     * @throws Exception
     */
    public function getStack()
    {
        $query = HU::setQueryParams(/** @lang sql */
            "SELECT * FROM {$this->getStacksTable()} WHERE clid = :clid AND id = :stack_id",
            [
                ':clid' => $this->clid,
                ':stack_id' => $this->stackId,
            ]);

        return $this->getQueryOne($query);
    }

    /**
     * @param int $elementId
     * @param int $userId
     * @param int $autoPress
     * @return bool
     * @throws Exception
     */
    public function refuseElement(int $elementId, int $userId, int $autoPress)
    {
        $element = $this->getElement($elementId);
        $pressText = $autoPress ? 'автоматически' : 'принудительно';

        $this->message("Пользователь $userId $pressText отклонил элемент $elementId:");
        $this->message($element);

        if (!$element) {
            $this->message('сделки нет в бд');

            return false;
        }

        if ($element['element_status'] === 'accepted') {
            $this->message('сделку уже приняли');

            return false;
        }

        if (!$autoPress) {
            $this->message("Ставим пользователю $userId метку об отказе со сроком действия 30 сек");
            $this->updateUserData($userId);

            $this->message('Обновляем метку времени для флага "в обработке"');
            $inUse = true;
        }

        $this->message("Меняем статус элемента $elementId на refused");
        $this->changeElement($elementId, $inUse ?? null, 'refused');

        $this->sendToLog('refused', $userId, $elementId, $element['element_type']);

        return true;
    }

    /**
     * Пользователь принимает элемент
     * @param $element
     * @param $elementId
     * @param $userId
     * @return bool
     * @throws Exception
     */
    public function acceptElement($element, $elementId, $userId)
    {
        $this->message("Пользователь $userId принял элемент:");
        $this->message($element);

        if (!$element) {
            $this->message('сделки нет в бд');

            return false;
        }

        if ($element['element_status'] === 'accepted') {
            $this->message('сделку уже приняли');

            return false;
        }

        $this->message("Добавляем n+1 в счетчик пользователя $userId, ставим метку об отказе на 3 минуты");
        $this->updateUserData($userId, 1);

        $this->message("Обнуляем все отметки об отправке для пользователей очереди [ $this->stackId ]");
        $this->removeSendFlagForAll($this->getStackUsers());

        $this->message("Меняем статус элемента $elementId на accepted");
        $this->changeElement($elementId, false, 'accepted', null, null, $userId);

        $this->sendToLog('accepted', $userId, $elementId, $element['element_type']);

        return true;
    }

    /**
     * @param $userId
     * @param null $accept
     * @return bool
     * @throws Exception
     */
    protected function updateUserData($userId, $accept = null)
    {
        $pause = 30;
        $increaseAccept = '';

        if ($accept) {
            $pause = 3 * 60;
            $increaseAccept = 'accepted = accepted + 1,';
        }

        $refuseTime = time() + $pause;

        // используется прямой запрос для инкремента accepted + 1, который
        // позволяет обходиться без лишнего запроса на получение accepted
        $query = /** @lang text */
            "UPDATE {$this->getCountsTable()} SET $increaseAccept refuse_time = $refuseTime WHERE stack_id = '{$this->stackId}' and user_id = $userId";

        return $this->executeQuery($query);
    }

    /**
     * @param string $action
     * @param int $userId
     * @param int $elementId
     * @param string $elementType
     * @return bool
     * @throws Exception
     */
    protected function sendToLog(string $action, int $userId, int $elementId, string $elementType)
    {
        $query = HU::setQueryParams(/* @lang text */
            "INSERT INTO {$this->getLogsTable()} SET
        action_type = :action_type,
        clid = :clid,
        user_id = :user_id,
        stack_id = :stack_id,
        date = :date,
        element_type = :element_type,
        element_id = :element_id",
            [
                ':action_type' => $action,
                ':clid' => $this->clid,
                ':user_id' => $userId,
                ':stack_id' => $this->stackId,
                ':date' => time(),
                ':element_type' => $elementType,
                ':element_id' => $elementId,
            ]);

        return $this->executeQuery($query);
    }

    /**
     * @param $elementId
     * @return array
     * @throws Exception
     */
    public function getElement(int $elementId)
    {
        $query = HU::setQueryParams(/* @lang text */
            "SELECT * FROM {$this->getElementsTable()} WHERE
        clid = :clid AND
        stack_id = :stack_id AND
        element_id = :element_id",
            [
                ':clid' => $this->clid,
                ':stack_id' => $this->stackId,
                ':element_id' => $elementId,
            ]);

        return $this->getQueryOne($query);
    }

    /**
     * @param $fieldId
     * @return bool
     */
    public function addAccessSpeedField($fieldId)
    {
        try {
            if (is_null($fieldId) || !is_int($fieldId)) {
                throw new Exception("Некорректное значение fieldId [ $fieldId ]");
            }

            $this->message('Создаем таблицу');
            $this->createTable();

            if (!$fieldId) {
                $fieldName = ACCESS_SPEED_FIELD_NAME;

                $this->message("Поля $fieldName нет, создаём его");

                $response = amocrm_CreateField($fieldName, 2, 2);

                $this->message($response);

                $fieldId = $response['response']['fields']['add'][0]['id'] ?? false;

                if (!$fieldId) {
                    throw new Exception('некорректный ответ от АМО: ' . json_encode($response));
                }

                $this->message('Id созданного поля: ' . $fieldId);
            }

            $this->updateFieldEntry($fieldId);
        } catch (Exception $exception) {
            $this->message('Ошибка при обработке поля: ' . $exception->getMessage());

            return false;
        }

        return true;
    }

    /**
     * @param int $dateCreate
     * @return array|null
     */
    public function handleAccessSpeedField(int $dateCreate)
    {
        try {
            $this->message('Получаем ID поля "' . ACCESS_SPEED_FIELD_NAME . '": ');

            $fieldId = $this->getAccessSpeedFieldId();

            if (!$fieldId) {
                throw new Exception('Поля не существует');
            }

            $this->message($fieldId);
        } catch (Exception $exception) {
            $this->message('Ошибка при получении поля: ' . $exception->getMessage());
            return null;
        }

        return [
            $fieldId => [
                'type' => 2,
                'value' => time() - $dateCreate,
                ],
        ];
    }

    /**
     * @param $fieldId
     * @return bool
     * @throws Exception
     */
    protected function updateFieldEntry(int $fieldId)
    {
        $query = HU::setQueryParams(/** @lang sql */
            "INSERT INTO {$this->getAccessSpeedTable()} 
            SET field_id = :field_id, clid = :clid 
            ON DUPLICATE KEY UPDATE field_id = :field_id", [
            ':clid' => $this->clid,
            ':field_id' => $fieldId,
        ]);

        return $this->executeQuery($query);
    }

    /**
     * @return int
     * @throws Exception
     */
    protected function getAccessSpeedFieldId(): int
    {
        $query = HU::setQueryParams(/** @lang sql */
            "SELECT field_id FROM {$this->getAccessSpeedTable()} WHERE
         clid = :clid", [
            ':clid' => $this->clid,
        ]);

        return $this->getQueryOne($query)['field_id'] ?? 0;
    }

    /**
     * @return bool
     * @throws Exception
     */
    protected function createTable()
    {
        $query = /** @lang sql */
            "CREATE TABLE IF NOT EXISTS {$this->getAccessSpeedTable()} (
    `clid` int(11) NOT NULL,
  `field_id` int(11) DEFAULT '0',
  `create` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`clid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4";

        return $this->executeQuery($query);
    }

    protected function getAccessSpeedTable()
    {
        return '`widget_uber_access_speed_field`';
    }
}
