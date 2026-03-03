import type { AxiosRequestConfig } from 'axios'
import axios from '@data-fair/lib-node/axios.js'

export const createOneGeoClient = (baseUrl: string, secrets: any) => {
  let token: string | null = null
  const client = axios.create({ baseURL: baseUrl })

  const authenticate = async () => {
    try {
      const response = await client.post('/login/signin/', {
        username: secrets.username,
        password: secrets.password
      })
      token = response.data.token
    } catch (error: any) {
      throw new Error(`Échec de l'authentification OneGeo: ${error.message}`)
    }
  }

  const executeRequest = async (config: AxiosRequestConfig) => {
    return await client.request({
      ...config,
      headers: {
        ...config.headers,
        Authorization: `JWT ${token}`,
        'Content-Type': 'application/json'
      }
    })
  }

  const request = async (config: AxiosRequestConfig) => {
    if (!token) {
      await authenticate()
    }

    try {
      return await executeRequest(config)
    } catch (error: any) {
      // if the error is due to an unauthorized status, try to re-authenticate and retry the request once
      if (error.response && [401, 403].includes(error.response.status)) {
        await authenticate()
        return await executeRequest(config)
      }
      throw error
    }
  }

  return { request }
}

export type OneGeoClient = ReturnType<typeof createOneGeoClient>
