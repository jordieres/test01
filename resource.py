"""
resource.py

First Prototype of Kubernetes oriented solution for the RESOURCE agent

Version History:
- 1.0 (2024-03-06): Initial version developed by Rodrigo Castro Freibott.
"""

import time
import argparse
import configparser
from multiprocessing import Pool
from common import VAction, TOPIC_GEN, sendmsgtopic
from data import get_resource_status
from functions import calculate_production_cost, get_new_resource_status
from agent import Agent


class Resource(Agent):
    """
    Class Resource supporting the plant agents.

    Arguments:
        topic     (str): Topic driving the relevant converstaion.
        agent     (str): Name of the agent creating the object.
        status   (dict): Status of the resource
        kafka_ip  (str): IP address and TCP port of the broker.
        counterbid_wait (float): Number of seconds granted for waiting confirmation.
        verbose   (int): Level of details being saved.

    """
    def __init__(self, topic: str, agent: str, status: dict, kafka_ip: str, counterbid_wait: float, verbose: int = 1):

        super().__init__(topic=topic, agent=agent, kafka_ip=kafka_ip, verbose=verbose)
        """
           Constructor function for the Resource Class

        :param str topic: Topic driving the relevant converstaion.
        :param str agent: Name of the agent creating the object.
        :param dict status: Status of the resource.
        :param str kafka_ip: IP address and TCP port of the broker.
        :param float counterbid_wait: Number of seconds granted for waiting confirmation.
        :param int verbose: Level of details being saved.
        """
        self.action_methods.update({
            'CREATE': self.handle_create_action, 'START': self.handle_start_action,
            'COUNTERBID': self.handle_counterbid_action, 'ASKCONFIRM': self.handle_askconfirm_action,
            'CONFIRM': self.handle_confirm_action, 'ASSIGNED': self.handle_assigned_action
        })

        self.round_number = 0
        self.status = status
        self.counterbid_wait = counterbid_wait

        self.iter_post_bid = 0
        self.bids = []
        self.last_bid_time = None
        self.bid_to_confirm = dict()
        self.previous_price = None

        if self.verbose > 1:
            self.write_log(f"Finished creating the agent {self.agent} with status {self.status}.")

    def move_to_next_round(self, material_params: dict):
        """
        Moves the resource to the next round by updating its status according to the assigned material's parameters,
        updating its name with the next round number, and starting the round.

        :param dict material_params:
        """
        self.status = get_new_resource_status(material_params=material_params, resource_status=self.status)
        roundless_name = self.agent[:self.agent.rfind(":")]
        self.round_number += 1
        self.agent = roundless_name + f":{self.round_number}"
        if self.verbose > 1:
            self.write_log(f"Moved resource {roundless_name} to round {self.round_number}. New status: {self.status}")

        self.iter_post_bid = 0
        self.bids = []
        self.last_bid_time = None
        self.bid_to_confirm = dict()

        full_msg = dict(
            source=self.agent, dest=self.agent, topic=self.topic, action='START',
            payload=dict(price=self.previous_price)
        )
        self.handle_start_action(full_msg)

    def handle_create_action(self, dctmsg: dict) -> str:
        """
        Handles the CREATE action. It clones the master RESOURCE for one auction.
        This instruction should only be given to the general RESOURCE.

        :param dict dctmsg: Message dictionary
        :return: Status of the handling
        :rtype: str
        """

        topic = dctmsg['topic']
        payload = dctmsg['payload']
        resource = payload['id']
        counterbid_wait = payload['counterbid_wait']
        agent = f"RESOURCE:{topic}:{resource}:0"
        status = get_resource_status(resource)

        init_kwargs = dict(
            topic=topic, agent=agent, status=status, kafka_ip=self.kafka_ip,
            counterbid_wait=counterbid_wait, verbose=self.verbose,
        )
        if self.verbose > 1:
            self.write_log(f"Creating resource with configuration {init_kwargs}...")

        pool = Pool(processes=2)
        pool.apply_async(create_resource, (init_kwargs,))
        pool.close()

        return 'CONTINUE'

    def handle_start_action(self, dctmsg: dict) -> str:
        """
        Handles the START action. It starts the RESOURCE's auction by instructing the MATERIALs to start bidding.
        This instruction should only be given to the RESOURCE children of the auction.

        :param dict dctmsg: Message dictionary
        :return: Status of the handling
        :rtype: str
        """
        topic = dctmsg['topic']
        payload = dctmsg['payload']
        previous_price = None
        if 'price' in payload:
            previous_price = payload['price']

        sendmsgtopic(
            producer=self.producer,
            tsend=topic,
            topic=topic,
            source=self.agent,
            dest="MATERIAL:" + topic + ":.*",
            action="BID",
            payload=dict(id=self.agent, status=self.status, previous_price=previous_price),
            vb=self.verbose
        )
        if self.verbose > 2:
            self.write_log(f"Instructed all MATERIAL:{topic} to bid")

        # Reset bidding status
        self.iter_post_bid = 0
        self.last_bid_time = time.perf_counter()

        return 'CONTINUE'

    def handle_counterbid_action(self, dctmsg: dict) -> str:
        """
        Handles the COUNTERBID action. It gets the material ID, its parameters, and its bidding price
        to calculate the profit (price minus producation cost) for the resource.
        This instruction should only be given to the RESOURCE children of the auction.

        :param dict dctmsg: Message dictionary
        :return: Status of the handling
        :rtype: str
        """
        payload = dctmsg['payload']
        material_params = payload['material_params']
        bidding_price = payload['price']

        prod_cost = calculate_production_cost(material_params=material_params, resource_status=self.status)
        if prod_cost is None:
            if self.verbose > 1:
                self.write_log(
                    f"Rejected offer from material {payload['id']}. "
                    f"The transition function to this material returns null"
                )
            return 'CONTINUE'

        profit = bidding_price - prod_cost
        bid = dict(material=payload['id'], profit=profit, price=bidding_price)

        # Reset bidding status
        self.iter_post_bid = 0
        self.last_bid_time = time.perf_counter()

        # Add the bid to the list
        self.bids.append(bid)
        if self.verbose > 1:
            self.write_log(f"Added {bid} to the list of bids, {self.bids}")

        return 'CONTINUE'

    def handle_askconfirm_action(self, dctmsg: dict) -> str:
        """
        Handles the ASKCONFIRM action. It asks the material for confirmation to settle the resource-material assignment.

        :param dict dctmsg: Message dictionary
        :return: Status of the handling
        :rtype: str
        """
        topic = dctmsg['topic']
        material = dctmsg['payload']['id']
        sendmsgtopic(
            producer=self.producer,
            tsend=topic,
            topic=topic,
            source=self.agent,
            dest=material,
            action="ASKCONFIRM",
            payload=dict(id=self.agent),
            vb=self.verbose
        )
        return 'CONTINUE'

    def handle_confirm_action(self, dctmsg: dict) -> str:
        """
        Handles the CONFIRM action. It receives the confirmation from the material and moves to the next round.
        This instruction should only be given to the RESOURCE children of the auction,
        and only from the MATERIAL that the resource previously asked for confirmation.

        :param dict dctmsg: Message dictionary
        :return: Status of the handling
        :rtype: str
        """
        payload = dctmsg['payload']
        material = payload['id']
        material_params = payload['material_params']

        if material != self.bid_to_confirm['material']:
            error_msg = (
                f"The sender of the confirmation {material} does not match "
                f"the pending material {self.bid_to_confirm['material']}"
            )
            if self.verbose > 1:
                self.write_log(f"ERROR: {error_msg}")
            raise RuntimeError(error_msg)

        self.previous_price = self.bid_to_confirm['price']
        if self.verbose > 1:
            self.write_log(
                f"The resource will be assigned to material {material} with bid {self.bid_to_confirm}. "
                f"Moving to the next round..."
            )
        self.move_to_next_round(material_params=material_params)
        return 'CONTINUE'

    def handle_assigned_action(self, dctmsg: dict) -> str:
        """
        Handles the ASSIGNED action. It removes the material from the list of bids or,
        if the material matches the pending material, it no longer waits for its confirmation.
        This instruction should only be given to the RESOURCE children of the auction.

        :param dict dctmsg: Message dictionary
        :return: Status of the handling
        :rtype: str
        """
        payload = dctmsg['payload']
        material = payload['id']

        if self.bid_to_confirm and material == self.bid_to_confirm['material']:
            if self.verbose > 1:
                self.write_log(f"The material {material} has been assigned to another resource.")
            self.bid_to_confirm = dict()
            return 'CONTINUE'

        for index, bid in enumerate(self.bids):
            if bid['material'] == material:
                self.bids.pop(index)
                if self.verbose > 1:
                    self.write_log(f"Removed the assigned material, {material}, from the list of bids: {self.bids}")
        return 'CONTINUE'

    def callback_before_message(self) -> str:
        """
        Checks the bidding and confirmation status
        :return: Status of the callback
        :rtype: str
        """
        if self.last_bid_time is None:
            return 'CONTINUE'

        # A bid has already happened; increment the number of iterations post-bid
        self.iter_post_bid += 1
        time_after_bid = time.perf_counter() - self.last_bid_time
        if (self.verbose > 1) and ((self.iter_post_bid - 1) % 15 == 0):
            self.write_log(
                f"Iteration {self.iter_post_bid - 1} after bid. Passed {time_after_bid:.2f}s after last bid"
            )

        # Do not proceed if not enough time has passed since the last bid or
        # the resource is waiting for a material to confirm
        if time_after_bid < self.counterbid_wait or self.bid_to_confirm:
            return 'CONTINUE'

        # Kill this resource child if there are no more materials to ask
        if len(self.bids) == 0:
            if self.verbose > 1:
                self.write_log(
                    f"No more bids to process. The resource will not be assigned to any material. "
                    f"Killing the agent..."
                )
            return 'END'

        # Ask the (next) best material for confirmation
        self.bids.sort(key=lambda item: item['profit'])
        best_bid = self.bids.pop()
        if self.verbose > 1:
            self.write_log(f"Removed the best bid, {best_bid}, from the list of bids: {self.bids}")

        dctmsg = dict(topic=self.topic, payload=dict(id=best_bid['material']))
        self.process_message(action='ASKCONFIRM', dctmsg=dctmsg)

        self.bid_to_confirm = best_bid
        if self.verbose > 1:
            self.write_log(f"Asked material {best_bid['material']} for confirmation.")

        return 'CONTINUE'


def create_resource(init_kwargs: dict)-> None:
    """
    Helper function to create Resource instances asynchronously with Python's multiprocessing.
    The reason for this function is that multiprocessing can handle functions but not methods.
    Source: https://stackoverflow.com/questions/41000818/can-i-pass-a-method-to-apply-async-or-map-in-python-multiprocessing

    :param dict init_kwargs:
    """

    resource = Resource(**init_kwargs)
    resource.follow_topic()


def main():
    """
    Create the general RESOURCE and make it follow the general topic

    :param str base: Path for the configuration file. Passed in the command line
    :param int verbose: Option to print information. Passed in the command line
    """

    # Extract the 'verbose' command line argument
    # The value in 'base' indicates the path to the configuration file
    # where the IP will be read
    ap = argparse.ArgumentParser()
    ap.add_argument("-b", "--base", type=str, dest="base", required=True,
                    help="Path from current place to find config.cnf file")
    ap.add_argument("-v", "--verbose", nargs='?', action=VAction,
                    dest='verbose', help="Option for logging detailed information")
    args = vars(ap.parse_args())
    verbose = 0
    if args["verbose"]:
        verbose = args["verbose"]
    base = "."
    if args["base"]:
        base = args["base"]
    if verbose > 0:
        print(f"Running program with {verbose=}, {base=}.")

    # Global configuration
    config = configparser.ConfigParser()
    config.read(base + '/config.cnf')
    kafka_ip = config['DEFAULT']['IP']

    # The parameter 'counterbid_wait' is irrelevant in the main resource
    main_resource = Resource(
        topic=TOPIC_GEN, agent=f"RESOURCE:{TOPIC_GEN}", status=dict(), kafka_ip=kafka_ip,
        counterbid_wait=15, verbose=verbose
    )
    main_resource.follow_topic()


if __name__ == '__main__':

    main()

