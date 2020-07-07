/**
 * Copyright 2020 IBM
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module.exports = {
    PORT: process.env.PORT,
    DB2WHREST_PROTOCOL: process.env.DB2WHREST_PROTOCOL,
    DB2WHREST_HOST: process.env.DB2WHREST_HOST,
    DB2WHREST_PORT: process.env.DB2WHREST_PORT,
    DB2WHREST_QUERY_JSON: process.env.DB2WHREST_ENDPOINT + 'sql_query_json?',
    DB2WHREST_USER: process.env.DB2WHREST_USER,
    DB2WHREST_PASSWORD: process.env.DB2WHREST_PASSWORD,
};
