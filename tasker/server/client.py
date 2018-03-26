import requests
import pickle


class TaskerServerClient:
    def __init__(
        self,
        host,
        port,
    ):
        self.host = host
        self.port = port

        self.api_url = 'http://{host}:{port}/'.format(
            host=host,
            port=port,
        )

    def key_get(
        self,
        key,
    ):
        response = requests.post(
            url=self.api_url + 'key_get?key={key}'.format(
                key=key,
            ),
        )

        if response.status_code == 404:
            return None
        else:
            return response.content

    def key_set(
        self,
        key,
        value,
    ):
        response = requests.post(
            url=self.api_url + 'key_set?key={key}'.format(
                key=key,
            ),
            data=value,
        )

        key_is_new = response.status_code == 200

        return key_is_new

    def key_delete(
        self,
        key,
    ):
        response = requests.post(
            url=self.api_url + 'key_delete?key={key}'.format(
                key=key,
            ),
        )

        return response.ok is True

    def queue_push(
        self,
        queue_name,
        items,
        priority,
    ):
        response = requests.post(
            url=self.api_url + 'queue_push?queue_name={queue_name}&priority={priority}'.format(
                queue_name=queue_name,
                priority=priority,
            ),
            data=pickle.dumps(
                obj=items,
            )
        )

        if response.ok:
            return True
        else:
            return False

    def queue_pop(
        self,
        queue_name,
        number_of_items,
    ):
        response = requests.post(
            url=self.api_url + 'queue_pop?queue_name={queue_name}&number_of_items={number_of_items}'.format(
                queue_name=queue_name,
                number_of_items=number_of_items,
            ),
        )

        items = pickle.loads(
            data=response.content,
        )

        return items

    def queue_delete(
        self,
        queue_name,
    ):
        response = requests.post(
            url=self.api_url + 'queue_delete?queue_name={queue_name}'.format(
                queue_name=queue_name,
            ),
        )

        return response.ok is True

    def queue_length(
        self,
        queue_name,
    ):
        response = requests.post(
            url=self.api_url + 'queue_length?queue_name={queue_name}'.format(
                queue_name=queue_name,
            ),
        )
        queue_number_of_items = int(response.content)

        return queue_number_of_items

    def set_add(
        self,
        set_name,
        value,
    ):
        response = requests.post(
            url=self.api_url + 'set_add?set_name={set_name}'.format(
                set_name=set_name,
            ),
            data=value,
        )
        item_new_in_set = response.status_code == 200

        return item_new_in_set

    def set_remove(
        self,
        set_name,
        value,
    ):
        response = requests.post(
            url=self.api_url + 'set_remove?set_name={set_name}'.format(
                set_name=set_name,
            ),
            data=value,
        )
        item_removed_from_set = response.status_code == 200

        return item_removed_from_set

    def set_contains(
        self,
        set_name,
        value,
    ):
        response = requests.post(
            url=self.api_url + 'set_contains?set_name={set_name}'.format(
                set_name=set_name,
            ),
            data=value,
        )
        item_exists_in_set = response.status_code == 200

        return item_exists_in_set

    def set_flush(
        self,
        set_name,
    ):
        response = requests.post(
            url=self.api_url + 'set_flush?set_name={set_name}'.format(
                set_name=set_name,
            ),
        )
        set_flushed = response.status_code == 200

        return set_flushed
