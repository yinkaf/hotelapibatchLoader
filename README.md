# hotelapibatchLoader

Hotel rest data upload

This command line application allows Hotels and associated Rooms to be loaded in a batch.
Hotels and Room data must be in a JSON file with the following structure.
```json
  [
  {
    "name": "Hotel1",
    "description": "Hotel 1 description",
    "cityCode": "lon",
    "rooms": [
      {
        "description": "Basic Room"
      },{
        "description": "Double room"
      },{
        "description": "Executive Room"
      }
    ]
  },

  {
    "name": "Hotel2",
    "description": "Hotel 2 description",
    "cityCode": "lon",
    "rooms": [
      {
        "description": "Basic Room"
      },{
        "description": "Double room"
      },{
        "description": "Executive Room"
      }
    ]
  }
  ]
  ```
