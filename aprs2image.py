#!/usr/bin/python3

import argparse
import base64
from influxdb import InfluxDBClient

# Command line input
parser = argparse.ArgumentParser(description='Connects to InfluxDB and saves out image')
parser.add_argument('--dbhost', help='Set InfluxDB host', default="localhost")
parser.add_argument('--dbport', help='Set InfluxDB port', default="8086")
parser.add_argument('--dbuser', help='Set InfluxDB user', default="admin")
parser.add_argument('--dbpw', help='Set InfluxDB password', default="admin")
parser.add_argument('--dbname', help='Set InfluxDB database name', default="aprs")

parser.add_argument('--callsign', help='Set APRS callsign (inc ssid) for image packets', required=True)
parser.add_argument('--since', help='How far back to process, eg 1h, 1d, 1w', default="1d")

parser.add_argument('--debug', help='Debug mode, print Debug level info', action='store_true')
parser.add_argument('--verbose', help='Print out every packet', action='store_true')
parser.add_argument('--test', help='Test mode, using test data', action='store_true')
parser.add_argument('--base64', help='Also write out base64 data', action='store_true')

# Parse the arguments
args = parser.parse_args()

# Functions
def processImage(imageName, imageData, packetNum):
    """Process Base64 data into an image

    imageName = name of image excluding extension
    imageData = Base64 encoded image data
    """

    # packetNum starts at 0, so number of packets to process is packetNum + 1
    print(f'Image {imageName} Processing, {packetNum + 1} packets.')

    imageName = './output_files/' + imageName + '.jpg'

    # My test data is not BASE64 encoded
    if args.test:
        print(imageData)
    else:
        with open(imageName, "wb") as imageFile:
            print(f'Saving image data to {imageName}.')
            imageFile.write(base64.b64decode(imageData.encode()))

    if args.base64:
        imageName = imageName[0:-3] + 'base64'
        with open(imageName, "wb") as imageFile:
            print(f'Saving base64 image data to {imageName}.')
            imageFile.write(imageData.encode())

    print()


def main():
    """Main function of aprs2image

    Read image packets for callsign from InfluxDB and save as images.
    """

    print('aprs2image started.')

    InfluxDB = InfluxDBClient(host=args.dbhost, port=args.dbport, username=args.dbuser, password=args.dbpw, database=args.dbname)

    # QueryResultSet is an object of type https://influxdb-python.readthedocs.io/en/latest/api-documentation.html#resultset
    QueryResultSet = InfluxDB.query('SELECT status FROM packet WHERE \"from\" = \'{call}\' and time >= now() - {since} order by time asc'
        .format(call = args.callsign, since = args.since)) # , epoch='m'

    #print(QueryResultSet)

    if args.test:
        # Simple test data with 2 missing packets and 1 duplicate packet
        QueryGenerator = [{'time': '111', 'status': 'c0272727'}, {'time': '212', 'status': 'e0282828'}, 
            {'time': '313', 'status': 'c0000000'}, {'time': '414', 'status': 'c0011111'}, {'time': '424', 'status': 'c0011111'}, 
            {'time': '515', 'status': 'c0022222'}, {'time': '717', 'status': 'c0055555'},
            {'time': '818', 'status': 'e0066666'}, {'time': '919', 'status': 'c0000000'}]
	# Packets to add if we get a missing packet
        blankPacket = '0' * 4
        PacketZero = '0000'
        PacketOne = '1111'
        PacketTwo = '2222'
        PacketThree = '3333'
        PacketFour = '4444'
    else:
        # Create a  generator for all the points in the ResultSet that match the given filters (none)
        QueryGenerator = QueryResultSet.get_points()
        # Packets to add if we get a missing packet, A is Base64 for 0
        blankPacket = 'A' * 200
        PacketZero = '/9j/4AAQSkZJRgABAQEAAAAAAAD/2wBDAAwICQsJCAwLCgsODQwOEh4UEhEREiUaHBYeLCYuLSsmKikwNkU7MDNBNCkqPFI9QUdKTU5NLzpVW1RLWkVMTUr/2wBDAQ0ODhIQEiMUFCNKMioySkpKSkpKSkpKSkpKSkpKSkpKSkpKSkpKSkpKSkpKSkpKSkpKSkpKSkpK'
        PacketOne = 'SkpKSkpKSkr/xAAfAAABBQEBAQEBAQAAAAAAAAAAAQIDBAUGBwgJCgv/xAC1EAACAQMDAgQDBQUEBAAAAX0BAgMABBEFEiExQQYTUWEHInEUMoGRoQgjQrHBFVLR8CQzYnKCCQoWFxgZGiUmJygpKjQ1Njc4OTpDREVGR0hJSlNUVVZXWFlaY2RlZmdoaWpzdHV2d3h5'
        PacketTwo = 'eoOEhYaHiImKkpOUlZaXmJmaoqOkpaanqKmqsrO0tba3uLm6wsPExcbHyMnK0tPU1dbX2Nna4eLj5OXm5+jp6vHy8/T19vf4+fr/xAAfAQADAQEBAQEBAQEBAAAAAAAAAQIDBAUGBwgJCgv/xAC1EQACAQIEBAMEBwUEBAABAncAAQIDEQQFITEGEkFRB2FxEyIygQgU'
        PacketThree = 'QpGhscEJIzNS8BVictEKFiQ04SXxFxgZGiYnKCkqNTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqCg4SFhoeIiYqSk5SVlpeYmZqio6Slpqeoqaqys7S1tre4ubrCw8TFxsfIycrS09TV1tfY2dri4+Tl5ufo6ery8/T19vf4+fr/wAARCAFAAeAD'
        # Only the first 31 Base64 character of Packet 4 are part of the header, used black image for the rest
        PacketFour = 'ASEAAhEBAxEB/9oADAMBAAIRAxEAPwDzs7M7vm96eWPUOMdhurl+IrQiYf3s0KdrcMRT8h6i4wRj+dDEkdc/U0xIQk48vNDfKAcqfpVIT8hflzxjFIMfn2zU3Y3cd0B+Y/TNJweOKfS47BwPf6Gk7D/CkT6gduRyCBSDGeOaQhQ2OO3pScc4xQCF7Yzmm9OavyGKDxxS'


    # Initialise variables
    imageName = "False"
    lastPacketNum = -1
    imageData = ''
    process = False

    # Loop around all packets found and generate images
    for packet in QueryGenerator:
        if args.verbose: print("Packet: " + packet['time'] + " " + packet['status'][0:15])
        #print(packet['status'])
        #print(packet['time'])
        #print(packet['status'][1:4])
        #print(packet['status'][0:4])

        # packet['status'] format = cXXXABC... or eXXXABC... where XXX is packet number starting at 000 
        # and e signifies the last packet of an image
        packetNum = int(packet['status'][1:4])

        # Have we found the first packet of an image?
        if packetNum == 0:

            # Are we already rx'ing an image?  If so we must have missed the end, 
            # so process that image and reset ready for this next image
            if imageName != "False":
                print('Missing end packet of last image so processing last image now.')
                processImage(str(imageName), imageData, lastPacketNum)

                # Reset ready for next image
                imageName = "False"
                lastPacketNum = -1
                imageData = ''
                process = False

            # packet['time'] format = 2022-11-26T15:18:59.959801Z
            imageName = packet['time'][0:16] + packet['time'][-1]
            if args.debug: print(f'Image {imageName} start found.')


        # Worth recovering up to 5 missing start packets as these are standard headers
        #TODO save last image if end packet and start packets missing
        if packetNum > 0 and packetNum < 5 and imageName == "False":

            # packet['time'] format = 2022-11-26T15:18:59.959801Z
            imageName = packet['time'][0:16] + packet['time'][-1]
            if args.debug: print(f'Image {imageName} late start found.')



        # If we are inside an image then add the packet to the imageData, excluding the packet header (eg c000)
        if imageName != "False":

            # duplicate packet?
            if packetNum == lastPacketNum:
                print(f'Image {imageName} expected packet {lastPacketNum + 1} got duplicate of packet {lastPacketNum}.')
                # Exit this iteration of the for loop to throw away this duplicate packet but then continue processing packets
                continue

            # missing packet(s)?
            if packetNum > (lastPacketNum + 1):
                missingPackets = packetNum - (lastPacketNum + 1)

                # If missing packet 0 - 4, then replace with known good data, otherwise we can only replace with blankPackets and hope 
                if lastPacketNum + 1 == 0 and missingPackets > 0:
                    print(f'Image {imageName} expected packet {lastPacketNum + 1} got packet {packetNum}, adding saved PacketZero.')
                    imageData += PacketZero
                    lastPacketNum += 1
                    missingPackets -= 1
                if lastPacketNum + 1 == 1 and missingPackets > 0:
                    print(f'Image {imageName} expected packet {lastPacketNum + 1} got packet {packetNum}, adding saved PacketOne.')
                    imageData += PacketOne
                    lastPacketNum += 1
                    missingPackets -= 1
                if lastPacketNum + 1 == 2 and missingPackets > 0:
                    print(f'Image {imageName} expected packet {lastPacketNum + 1} got packet {packetNum}, adding saved PacketTwo.')
                    imageData += PacketTwo
                    lastPacketNum += 1
                    missingPackets -= 1
                if lastPacketNum + 1 == 3 and missingPackets > 0:
                    print(f'Image {imageName} expected packet {lastPacketNum + 1} got packet {packetNum}, adding saved PacketThree.')
                    imageData += PacketThree
                    lastPacketNum += 1
                    missingPackets -= 1
                if lastPacketNum + 1 == 4 and missingPackets > 0:
                    print(f'Image {imageName} expected packet {lastPacketNum + 1} got packet {packetNum}, adding saved PacketFour.')
                    imageData += PacketFour
                    lastPacketNum += 1
                    missingPackets -= 1

                if missingPackets != 0:
                    print(f'Image {imageName} expected packet {lastPacketNum + 1} got packet {packetNum}, adding {missingPackets} blank packets.')
                    for _ in range(missingPackets): 
                        imageData += blankPacket

            # missed the image end packet? (ie packetNum went backwards)
            # this only happens if we have a missing end packet, and a missing image start packet
            if packetNum < lastPacketNum:
                print(f'Image {imageName} missed last packet, processing image now anyway.')
                # Don't add this packet to this image
                process = True
            else:
                # Safe to add packet to imageData if we got here
                imageData += packet['status'][4:]

            # Update lastPacketNum number
            lastPacketNum = packetNum

            # If we have reached the end of an image then process it and prepare to find the next image
            if packet['status'][0] == 'e' or process == True:
                processImage(str(imageName), imageData, packetNum)

                # Reset ready for next image
                imageName = "False"
                lastPacketNum = -1
                imageData = ''
                process = False

        else:
            if args.debug: print(f'Skipping packet {packet["status"][0:4]}.')




if __name__ == '__main__':
    main()


