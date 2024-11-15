pipeline {
    agent {
        docker { image 'python:3.10.15-alpine3.20' }
    }
    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                pip install -r requirements-local.txt
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
                sh 'python3 ShortTermPlanning.py -v 3 -b . -rw 10 -cw 30 -aw 50 -bw 15 -ew 10 -r 6 -n 1'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
            }
        }
    }
}