const fs = require('fs')
const zlib = require('zlib')
const readline = require('readline')
const moment = require('moment')
// const db = require('./mongodb')

const dir = '/amazon_data/blueplanet'
// const filenames = fs.readdirSync(dir).map(filename => `${dir}/${filename}`)
const filenames = ['./testdata.gz', './testdata2.gz']
// const filenames = [`${dir}/blueplanet-20190304.gz`, `${dir}/blueplanet-20190305.gz`, `${dir}/blueplanet-20190306.gz`]

let topicTrends = {}
let tagTrends = []
let count = 0

let roomsTrends = {}
let roomsList = []
let nbOfRoom = 1

const calcTrends = async (filename) => {
  const gzFileInput = fs.createReadStream(filename)
  const gunzip = zlib.createGunzip()

  const interface = readline.createInterface({
    input: gunzip,
  })

  trends = await new Promise((resolve, reject) => {
    console.log(moment().format(), `Start reading ${filename}`)
    interface.on('line', line => {
      if (!line.includes('start'))
        return
      const event = JSON.parse(line)

      const pageUrl = event.url
      let {rooms = []} = event
      rooms = Object.values(rooms)
      
      const topicId = event.topic_id
      let { tags = [] } = event
      tags = Object.values(tags)

      if ((event.event == 'start' || topicId === 0 || topicId === '0') && pageUrl.includes("forum")) {
        var prompt = pageUrl
        var forum = prompt.split("/") // forum[4] is room's name.
        forum = forum[4]
        if (forum) {
            if(Object.keys(roomsTrends).includes(forum)) {
              forum.viewer++
            }else {
              roomsTrends[forum] = {viewer: 1, url: prompt, forum}
            }
        } else {
          return
        }
      } else {
        return
      }
    })
    interface.on('error', (err) => {
      reject(err)
    })
    interface.on('close', () => {
      resolve({
        roomsList: roomsList
      })
    })
    gzFileInput.on('data', function (data) {
      gunzip.write(data)
    })
    gzFileInput.on('end', function () {
      gunzip.end()
    })
  })
  count++
  console.log(moment().format(), `Done reading ${filename} (${count}/${filenames.length})`)
  return trends
}

// -------------------------------------- main --------------------------------------

const main = async () => {
  const startTime = moment().format()
  console.log(startTime, 'Start')
  let result = `${startTime}, Start`

  const roomsList = []

  for (const filename of filenames) {
    await calcTrends(filename)
  }

  console.log('\n----------------------\n')
  roomsTrends = Object.values(roomsTrends)
  roomsTrends.sort((a, b) => b.viewer - a.viewer)
  roomsList.sort((a, b) => b.viewer - a.viewer)

  const topMostAccessedRooms = 'Top most being accessed rooms'
  console.log(topMostAccessedRooms)
  result += '\n\n' + topMostAccessedRooms
  for (let i = 0; i < 3; i++) {
    const {
      url,
      viewer,
      forum
    } = roomsTrends[i]
    const topRoomsString = `${i + 1}.)${forum} room, ${url} being accessed ${viewer} times.`
    console.log(topRoomsString)
    result += '\n' + topRoomsString
  }

  const endTime = moment()
  const endString = `\n${endTime.format()} Done. Running time: ${endTime.diff(startTime, 'seconds')} seconds.`
  console.log(endString)
  result += '\n' + endString + '\n----------------------\n'

  fs.appendFileSync('./result_rooms.txt', result)
}

main()