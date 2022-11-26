#!/usr/bin/python3

import argparse
import base64
from influxdb import InfluxDBClient

# Command line input
parser = argparse.ArgumentParser(description='Connects to InfluxDB and saves out images')
parser.add_argument('--dbhost', help='Set InfluxDB host', default="localhost")
parser.add_argument('--dbport', help='Set InfluxDB port', default="8086")
parser.add_argument('--dbuser', help='Set InfluxDB user', default="admin")
parser.add_argument('--dbpw', help='Set InfluxDB password', default="admin")
parser.add_argument('--dbname', help='Set InfluxDB database name', default="aprs")

parser.add_argument('--callsign', help='Set APRS callsign (inc ssid) for image packets', required=True)
parser.add_argument('--since', help='How far back to process, eg 1h, 1d, 1w', default="1d")

parser.add_argument('--debug', help='Debug mode, print Debug level info', action='store_true')
parser.add_argument('--test', help='Test mode, using test data', action='store_true')

# Parse the arguments
args = parser.parse_args()

# Functions
def processImage(imageName, image):
    """Process Base64 data into an image

    imageName = name of image excluding extension
    image = Base64 encoded image data
    """

    imageName = './output_files/' + imageName + '.jpg'
    with open(imageName, "wb") as imageFile:
        print(f'Saving image data to {imageName}.')
        imageFile.write(base64.b64decode(image.encode()))


def main():
    """Main function of aprs2image

    Read image packets for callsign from InfluxDB and save as images.
    """

    print('aprs2image started.')

    InfluxDB = InfluxDBClient(host=args.dbhost, port=args.dbport, username=args.dbuser, password=args.dbpw, database=args.dbname)

    # QueryResultSet is an object of type https://influxdb-python.readthedocs.io/en/latest/api-documentation.html#resultset
    QueryResultSet = InfluxDB.query('SELECT status FROM packet WHERE \"from\" = \'{call}\' and time >= now() - {since} order by time asc'
        .format(call = args.callsign, since = args.since), epoch='s')

    #print(QueryResultSet)

    if args.test:
        # Simple test data with 2 missing packets and 1 duplicate packet
        QueryGenerator = [{'time': 111, 'status': 'c0272727'}, {'time': 212, 'status': 'e0282828'}, 
            {'time': 313, 'status': 'c0000000'}, {'time': 414, 'status': 'c0011111'}, {'time': 424, 'status': 'c0011111'}, 
            {'time': 515, 'status': 'c0022222'}, {'time': 717, 'status': 'c0055555'},
            {'time': 818, 'status': 'e0066666'}, {'time': 919, 'status': 'c0000000'}]
	# Packet to add if we get a missing packet
        blankPacket = '0' * 4
    else:
        # Create a  generator for all the points in the ResultSet that match the given filters (none)
        QueryGenerator = QueryResultSet.get_points()
        # Packet to add if we get a missing packet
        blankPacket = '0' * 200

    # Initialise variables
    imageName = False
    lastPacket = -1
    image = ''
    process = False

    # Loop around all packets found and generate images
    for packet in QueryGenerator:
        #print(packet)
        #print(packet['status'])
        #print(packet['time'])
        #print(packet['status'][1:4])
        #print(packet['status'][0:4])

        packetNum = int(packet['status'][1:4])

        # Have we found the first packet of an image?
        if packetNum == 0:
            imageName = packet['time']
            if args.debug: print(f'Image {imageName} start found.')

        # If we are inside an image then add the packet to the image, excluding the packet header (eg c000)
        if imageName > 0:

            # duplicate packet?
            if packetNum == lastPacket:
                print(f'Image {imageName} expected packet {lastPacket + 1} got duplicate of packet {lastPacket}.')
                continue

            # missing packet(s)?
            if packetNum > (lastPacket + 1):
                missingPackets = packetNum - (lastPacket + 1)
                print(f'Image {imageName} expected packet {lastPacket + 1} got packet {packetNum}, adding {missingPackets} blank packets.')
  
                for _ in range(missingPackets): 
                    image += blankPacket

            # missed the image end packet?
            if packetNum < lastPacket:
                print(f'Image {imageName} missed last packet, processing it now anyway.')
                # dont add this packet to this image
                # TODO add it to the next image
                process = True
            else:
                # Safe to add packet to image if we got here
                image += packet['status'][4:]

            # Update lastPacket number
            lastPacket = packetNum

            # If we have reached the end of an image then process it and prepare to find the next image
            if packet['status'][0] == 'e' or process == True:
                if args.debug: print(f'Image {imageName} Processing, {packetNum} packets.')
                if args.test: print(image)

                processImage(str(imageName), image)

                # Reset ready for next image
                imageName = False
                lastPacket = -1
                image = ''
                process = False

        else:
            if args.debug: print(f'Skipping packet {packet["status"][0:4]}.')




if __name__ == '__main__':
    main()


