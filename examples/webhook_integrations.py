from ethvigil.EVCore import *

evc = EVCore(verbose=False)

# put in a contract address deployed from your EthVigil account
contract_instance = evc.generate_contract_sdk(
    contract_address='0xContractAddress',
    app_name='microblog'
)

# the URL to which event updates on the smart contract will be delivered by EthVigil
callback_url = 'https://webhook.yourcallback.URL'

# EthVigil will watch over 'NewPost' event updates.
# Events to be monitored are specified as a list of event names
print(contract_instance.add_event_integration(events=['NewPost'], callback_url=callback_url))

# EthVigil will watch over 'all' event updates. Hence the '*' being passed in the expected list of events
# print(contract_instance.add_event_integration(events=['*'], callback_url=callback_url))

# list all registered integrations - webhook, email, slack etc
# print(contract_instance.integrations)
