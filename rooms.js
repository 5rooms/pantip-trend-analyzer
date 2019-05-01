const fs = require('fs')
const zlib = require('zlib')
const readline = require('readline')
const moment = require('moment')
const db = require('./mongodb')

const dir = '/amazon_data/blueplanet'
// const filenames = fs.readdirSync(dir).map(filename => `${dir}/${filename}`)
const filenames = ['./testdata.gz', './testdata2.gz']
// const filenames = [`${dir}/blueplanet-20190304.gz`, `${dir}/blueplanet-20190305.gz`, `${dir}/blueplanet-20190306.gz`]

let topicTrends = {}
let roomTrends = []
let count = 0

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

      const topicId = event.topic_id
      let {
        rooms = []
      } = event
      rooms = Object.values(rooms)
      if (event.event !== 'start' || topicId === 0 || topicId === '0')
        return
      if (topicTrends[topicId] !== undefined) {
        if (!(event.mid === '0' || event.mid === 0) && !topicTrends[topicId].mids.includes(event.mid)) {
          topicTrends[topicId].viewer++
          topicTrends[topicId].mids.push(event.mid)
          if (!rooms || rooms === undefined || typeof rooms !== 'object' || rooms.length < 1)
            return
          try {
            rooms.forEach(room => {
              const roomFind = roomTrends.find(t => t.room === room)
              if (roomFind) {
                roomFind.viewer++
                if (!roomFind.topics.includes(topicId))
                  roomFind.topics.push(topicId)
              } else {
                roomTrends.push({
                  room,
                  viewer: 1,
                  topics: [topicId]
                })
              }
            })
          } catch (err) { }
        } else if (!(event.tc === '0' || event.tc === 0) && !topicTrends[topicId].tcs.includes(event.tc)) {
          topicTrends[topicId].viewer++
          topicTrends[topicId].mids.push(event.tc)
          if (!rooms || rooms === undefined || typeof rooms !== 'object' || rooms.length < 1)
            return
          try {
            rooms.forEach(room => {
              const roomFind = roomTrends.find(t => t.room === room)
              if (roomFind) {
                roomFind.viewer++
                if (!roomFind.topics.includes(topicId))
                  roomFind.topics.push(topicId)
              } else {
                roomTrends.push({
                  room,
                  viewer: 1,
                  topics: [topicId]
                })
              }
            })
          } catch (err) {

          }
        }
      } else {
        topicTrends[topicId] = {
          topic_id: topicId,
          updated_time: event.updated_time,
          rooms: event.rooms,
          rooms: event.rooms,
          viewer: 1,
          mids: event.mid && event.mid !== '0' ? [event.mid] : [],
          tcs: event.tc && event.tc !== '0' ? [event.tc] : []
        }
        if (!rooms || rooms === undefined || typeof rooms !== 'object' || rooms.length < 1)
          return
        try {
          rooms.forEach(room => {
            const roomFind = roomTrends.find(t => t.room === room)
            if (roomFind) {
              roomFind.viewer++
              if (!roomFind.topics.includes(topicId))
                roomFind.topics.push(topicId)
            } else {
              roomTrends.push({
                room,
                viewer: 1,
                topics: [topicId]
              })
            }
          })
        } catch (err) {

        }
      }

    })
    interface.on('error', (err) => {
      reject(err)
    })
    interface.on('close', () => {
      resolve({
        roomTrends: roomTrends
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

  const roomTrends = []

  for (const filename of filenames) {
    await calcTrends(filename)
  }

  console.log('\n----------------------\n')
  topicTrends = Object.values(topicTrends)
  topicTrends.sort((a, b) => b.viewer - a.viewer)
  roomTrends.sort((a, b) => b.viewer - a.viewer)

  /* Start of room trend */

  // const topMostPopularrooms = 'Top most popular rooms'
  // console.log(topMostPopularrooms)
  // result += '\n\n' + topMostPopularrooms
  // for (let i = 0; i < roomTrends.length; i++) {
  //   const { room, viewer, topics } = roomTrends[i]
  //   const toproomString = `${i + 1}.) ${room}, from ${viewer} viewers in ${topics.length} topics`
  //   console.log(toproomString)
  //   result += '\n' + toproomString
  // }

  /* End of room trend */

  const topMostPopularTopics = 'Top most popular topics'
  console.log(topMostPopularTopics)
  result += '\n\n' + topMostPopularTopics
  for (let i = 0; i < 10; i++) {
    const {
      topic_id,
      viewer,
      rooms
    } = topicTrends[i]
    const topTopicString = `${i + 1}.) ${topic_id}, from ${viewer} viewers from [${rooms}] rooms.`
    console.log(topTopicString)
    result += '\n' + topTopicString
  }

  const endTime = moment()
  const endString = `\n${endTime.format()} Done. Running time: ${endTime.diff(startTime, 'seconds')} seconds.`
  console.log(endString)
  result += '\n' + endString + '\n----------------------\n'

  fs.appendFileSync('./result.txt', result)
}

main()
