import yaml
import os


class YamlParser():
    def __init__(self):
        return None

    def read_yaml(self):
        with open("ibm_presto_release_version.yaml", "r") as stream:
            try:
                file_data = yaml.safe_load(stream)
                return file_data
            except yaml.YAMLError as exc:
                print(exc)
                return 1

    def edit_version(self, content):
        current_version = content["ibm_presto_version"].split('.')
        v_num = int(current_version[-1])
        updated_v_num = v_num + 1
        current_version[-1] = str(updated_v_num)
        updated_version = '.'.join(current_version)
        print(updated_version)
        content["ibm_presto_version"] = updated_version
        return content

    def write_yaml(self, updated_version):
        with open('ibm_presto_release_version.yaml', 'w', encoding='utf8') as outfile:
            try:
                yaml.dump(updated_version, outfile, default_flow_style=False, allow_unicode=True)
                return 0
            except yaml.YAMLError as exc:
                print(exc)
                return 1

    def main(self):
        try:
            current_version = self.read_yaml()
            print("current_version : ", current_version)
            updated_version = self.edit_version(current_version)
            print("updated_version : ", updated_version)
            self.write_yaml(updated_version)
        except Exception as err:
            return err


if __name__ == '__main__':
    base_path = os.path.realpath(os.path.dirname(__file__))
    yamal_parser = YamlParser()
    yamal_parser.main()
