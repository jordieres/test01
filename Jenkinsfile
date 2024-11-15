pipeline {
    agent {label 'docker-machine'}
    
    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                sh 'python3 --version'
                sh 'docker --version'
            }
        }
        stage('Run Tests') {
            parallel {
                stage('Test1') {
                    steps {
                        echo 'Testing Step 1'
                        sh 'python3 ShortTermPlanning.py -v 3 -b . -rw 10 -cw 30 -aw 50 -bw 15 -ew 10 -r 6 -n 1'
                        sh '[[ $(ls -A /var/log/dynreact-logs/) ]] && echo "contains files" || echo "Empty folder"; exit 1'
                    }
                }
                stage('Test2') {
                    steps {
                        echo 'Testing Step 2'
                        sh 'python3 ShortTermPlanning.py -v 3 -b . -rw 10 -cw 30 -aw 50 -bw 15 -ew 10 -r 6 -n 2'
                        sh '[[ $(ls -A /var/log/dynreact-logs/) ]] && echo "contains files" || echo "Empty folder"; exit 1'
                    }
                }
                stage('Test3') {
                    steps {
                        echo 'Testing Step 3'
                        sh 'python3 ShortTermPlanning.py -v 3 -b . -rw 10 -cw 30 -aw 200 -bw 15 -ew 10 -r 6 7 -n 1'
                        sh '[[ $(ls -A /var/log/dynreact-logs/) ]] && echo "contains files" || echo "Empty folder"; exit 1'
                    }
                }
                stage('Test4') {
                    steps {
                        echo 'Testing Step 4'
                        sh 'python3 ShortTermPlanning.py -v 3 -b . -rw 10 -cw 30 -aw 200 -bw 15 -ew 10 -r 6 7 -n 4'
                        sh '[[ $(ls -A /var/log/dynreact-logs/) ]] && echo "contains files" || echo "Empty folder"; exit 1'
                    }
                }
                stage('Test5') {
                    steps {
                        echo 'Testing Step 5'
                        sh 'python3 ShortTermPlanning.py -v 3 -b . -rw 100 -cw 300 -aw 1200 -bw 45 -ew 100 -r 6 7'
                        sh '[[ $(ls -A /var/log/dynreact-logs/) ]] && echo "contains files" || echo "Empty folder"; exit 1'
                    }
                }
            }
        }
    }
}