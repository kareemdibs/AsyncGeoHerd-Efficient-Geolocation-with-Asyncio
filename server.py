import sys
import asyncio
import argparse
import logging
import json
import aiohttp
import time

# List of server names
SERVERS = ["Bailey",
           "Bona",
           "Campbell",
           "Clark",
           "Jaquez"
]

# Port numbers for each server (unique to me)
PORTS = {
    "Bailey": 1,
    "Bona": 2,
    "Campbell": 3,
    "Clark": 4,
    "Jaquez": 5
}

# Communication paths between servers (outlined in the spec)
COMMS = {
    "Clark": ["Bona", "Jaquez"],
    "Campbell": ["Bailey", "Bona", "Jaquez"],
    "Bona": ["Bailey", "Clark", "Campbell"],
    "Bailey": ["Campbell", "Bona"],
    "Jaquez": ["Clark", "Campbell"]
}

# Host IP
HOST = '127.0.0.1'

# My personal API key for Google Places API
API_KEY = "1"

class Server:
    def __init__(self, name):
        self.name = name
        self.ip = '127.0.0.1'
        self.port = PORTS[name]
        self.cls = dict()    # Store client information
        logging.basicConfig(
            filename=f'{self.name}_log.txt',  # Log file name
            encoding='utf-8',                # Encoding format
            format='%(asctime)s %(levelname)-8s %(message)s',  # Log message format
            level=logging.DEBUG              # Logging level
        )

    async def startServer(self):
        self.loggingInformation(f"Starting the following server: {self.name}")
        s = await asyncio.start_server(
            self.handleClient, 
            self.ip, 
            self.port
        )
        async with s:
            await s.serve_forever()
        self.loggingInformation(f"Closing the following server: {self.name}")
        s.close()
        pass

    async def handleClient(self, r, w):
        while not r.at_eof():
            current_input = await r.readline()
            message = current_input.decode()
            if not message:
                continue
            # print(message)
            self.loggingInformation(f"Received the following message: {message}")
            arguments = message.split()
            resp = ""
            
            # Handle IAMAT command
            if arguments[0] == "IAMAT":
                if self.checkIAMAT(arguments):
                    _, client_id, coordinates, client_timestamp = arguments
                    time_difference = time.time() - float(client_timestamp)
                    tdstr = ["", "+"][time_difference > 0] + str(time_difference)
                    resp = f'AT {self.name} {tdstr} {client_id} {coordinates} {client_timestamp}'
                    self.cls[client_id] = {
                        'timestamp': client_timestamp,
                        'msg': resp
                    }
                    await self.propagateInfo(resp)
                else:
                    self.loggingError(f"Error while parsing {message}")
                    resp = "? " + message
            
            # Handle WHATSAT command
            elif arguments[0] == "WHATSAT":
                if self.checkWHATSAT(arguments):
                    _, client_id, radius, maxNumPlaces = arguments
                    at_message = self.cls[client_id]['msg']
                    coordinates = at_message.split()[4]
                    pl_details = await self.retrievePlaces(coordinates, radius, maxNumPlaces)
                    pl_details = pl_details.rstrip('\n')
                    resp = f'{at_message}\n{pl_details}\n\n'
                else:
                    self.loggingError(f"Error while parsing {message}")
                    resp = "? " + message
            
            # Handle AT command for propagation
            elif arguments[0] == "AT" and len(arguments) == 6:
                _, s, time_difference, client_id, coordinates, client_timestamp = arguments
                if not client_id in self.cls or client_timestamp > self.cls[client_id]['timestamp']:
                    at_message = message
                    self.cls[client_id] = {
                        'timestamp': client_timestamp,
                        'msg': at_message
                    }
                    self.loggingInformation(f"We have received new propagation information on {client_id}. Currently updating.")
                    await self.propagateInfo(at_message)
                else:
                    self.loggingInformation(f"We have already propagated the information to {client_id} so we will disregard it.")
            
            # Handle invalid commands
            else:
                self.loggingError(f"Error while parsing {message}")
                resp = "? " + message
            
            # Send response to the client
            if not resp:
                self.loggingInformation(f"There is no information to send to client. Continue operations.")
            else:
                self.loggingInformation(f"Sending response to client: {resp}")
                w.write(resp.encode())
            
            await w.drain()
            self.loggingInformation(f"Closing connection to client.")
            w.close()


    def checkIAMAT(self, cmd):
        _, _, coordinates, timestamp = cmd
        tc = coordinates.replace('+', '-')
        # print(timestamp)
        vals = list(filter(None, tc.split('-')))
        
        if (len(vals) != 2 or not (self.checkNumber(vals[0]) and self.checkNumber(vals[1]))) or not self.checkNumber(timestamp):
            return False
        else:
            return True

    
    def checkWHATSAT(self, command):
        _, client, radius, maxNumPlaces = command
        # print(radius)
        # print(maxNumPlaces)
        if (not (self.checkNumber(radius) and self.checkNumber(maxNumPlaces)) or 
            not (0 <= int(radius) <= 50) or 
            not (0 <= int(maxNumPlaces) <= 20) or 
            client not in self.cls):
                return False
        else:
            return True

    async def retrievePlaces(self, crds, radius, maxNumPlaces):
        async with aiohttp.ClientSession() as session:
            validatedCoordinates = self.verifyValidCoordinates(crds)
            if validatedCoordinates == None:
                self.loggingInformation("This is an invalid coordinate format. Please follow the correct coordinate formatting conventions.")
                sys.exit()
            
            url = f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={validatedCoordinates}&radius={str(float(radius)*1000)}&key={API_KEY}'
            
            self.loggingInformation(f"Fetching from API, please wait.")
            
            async with session.get(url) as response:
                newData = await response.json(loads=json.loads)
            
            self.loggingInformation(f'We have found {len(newData["results"])} places from the Google Places API.')
            
            if len(newData['results']) > int(maxNumPlaces):
                newData['results'] = newData['results'][:int(maxNumPlaces)]

            return str(json.dumps(newData, indent=4))

    def verifyValidCoordinates(self, coordinates):
        split_index = max(
            coordinates.rfind('+'),
            coordinates.rfind('-')
        )
        # print(split_index)
        return f'{coordinates[:split_index]},{coordinates[split_index:]}'

    async def propagateInfo(self, message):
        for adj in COMMS[self.name]:
            try:
                _, w = await asyncio.open_connection(self.ip, PORTS[adj])
                w.write(message.encode())
                self.loggingInformation(f"Propogating information to {adj}")
                await w.drain()
                w.close()
                await w.wait_closed()
                self.loggingInformation(f"Closing the connection to {adj}")
            except:
                self.loggingError(f"Failed to connect to {adj}")

    def checkNumber(self, value):
        try:
            float(value)
            return True
        except ValueError:
            return False

    def loggingInformation(self, message):
        logging.info(message)
        print(message)

    def loggingError(self, message):
        logging.error(message)
        print(message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser("CS131 Proxy Herd Server with asyncio")
    parser.add_argument(
        'server_name', 
        type=str, 
        help='Bailey, Bona, Campbell, Clark, or Jacquez'
    )
    args = parser.parse_args()
    # print("Confirming Connection (TEST).")
    if not (args.server_name in PORTS):
        print(f"This is not a valid server name: {args.server_name}")
        sys.exit()
    s = Server(args.server_name)
    try:
        asyncio.run(s.startServer())
    except KeyboardInterrupt:
        pass