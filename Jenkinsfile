pipeline {
    agent any
    
    stages {
        stage('Build') {
            steps {
                echo 'Building..'
                sh 'python --version'
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