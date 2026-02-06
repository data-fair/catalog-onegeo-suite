export { schema as configSchema, assertValid as assertConfigValid, type OneGeoSuiteConfig } from './catalogConfig/index.ts'
export { schema as importConfigSchema } from './importConfig/index.ts'

export type Link = {
  _main: boolean,
  name: string,
  description?: string,
  formats: string[],
  service?: string,
  url: string,
  projections?: string[]
}
